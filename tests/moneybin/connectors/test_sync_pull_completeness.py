"""A successful receipt requires retrieval evidence for every known native account."""

import json
from pathlib import Path
from typing import Any

import pytest

from moneybin.connectors.sync_models import (
    PlaidSourceResult,
    PullResult,
    SimpleFINSourceResult,
    SimpleFINV1OrgScope,
    UTCWindow,
)

FIXTURES = Path(__file__).parent / "fixtures" / "sync_v2"


def _completed_pull(protocol: str) -> PullResult:
    envelope = json.loads((FIXTURES / f"simplefin_{protocol}.json").read_text())
    source = envelope["sources"][0]
    del source["batches"]
    history: dict[str, Any] = {
        "observed_at": "2026-10-01T12:00:00Z",
        "replay_required": False,
        "discovery_history_target": None,
        "accounts": [
            {
                "account_id": outcome["account_id"],
                "native_scope": outcome["native_scope"],
                "acknowledged_intervals": [outcome["window"]],
                "history_target": None,
            }
            for outcome in source["outcomes"]
        ],
    }
    source["history"] = history
    return PullResult.model_validate({
        "job_id": envelope["job_id"],
        "ack_state": "confirmed",
        "setup_state": "complete",
        "refresh_state": "complete",
        "transactions_loaded": 0,
        "accounts_loaded": 0,
        "balances_loaded": 0,
        "transactions_removed": 0,
        "institutions": [],
        "source_results": [source],
        "sources": [
            {
                "provider": "simplefin",
                "connection_id": source["connection_id"],
                "credential_generation": source["credential_generation"],
                "protocol_version": source["protocol_version"],
                "history_phase": "post_ack",
                "history": {
                    **history,
                    "accounts": [
                        {
                            **account,
                            "account_id": "canonical-account",
                            "display_name": "Synthetic account",
                        }
                        for account in history["accounts"]
                    ],
                },
                "outcomes": [
                    {
                        **outcome,
                        "account_id": "canonical-account",
                        "display_name": "Synthetic account",
                        "scope_kind": "account",
                    }
                    for outcome in source["outcomes"]
                ],
            }
        ],
    })


@pytest.mark.parametrize("protocol", ["v1", "v2"])
def test_pull_complete_with_every_native_account_outcome(protocol: str) -> None:
    assert _completed_pull(protocol).complete


@pytest.mark.parametrize("protocol", ["v1", "v2"])
@pytest.mark.parametrize("missing", ["all", "one", "duplicate", "unexpected"])
def test_pull_incomplete_without_expected_native_account_outcomes(
    protocol: str, missing: str
) -> None:
    result = _completed_pull(protocol)
    source = result.source_results[0]
    assert isinstance(source, SimpleFINSourceResult)
    if missing == "all":
        source.outcomes = []
        result.sources[0].outcomes = []
    elif missing == "one":
        source.outcomes = source.outcomes[:1]
        result.sources[0].outcomes = result.sources[0].outcomes[:1]
    elif missing == "duplicate":
        source.outcomes[1] = source.outcomes[0].model_copy(deep=True)
    else:
        source.outcomes[1].account_id = "unexpected-native-account"

    assert not result.complete


def test_pull_complete_when_native_organization_label_changes() -> None:
    result = _completed_pull("v1")
    scope = result.source_results[0].outcomes[0].native_scope
    assert isinstance(scope, SimpleFINV1OrgScope)
    scope.org.name = "Renamed synthetic bank"

    assert result.complete


def test_pull_incomplete_without_expected_source_result() -> None:
    result = _completed_pull("v2")
    result.source_results = []

    assert not result.complete


def test_pull_incomplete_without_public_history_projection() -> None:
    result = _completed_pull("v2")
    result.sources = []

    assert not result.complete


@pytest.mark.parametrize("protocol", ["v1", "v2"])
@pytest.mark.parametrize(
    ("start", "end", "complete"),
    [
        ("2020-01-01", "2020-02-01", False),
        ("2026-09-02", "2026-10-01", True),
        ("2026-09-01", "2026-09-30", False),
        ("2026-09-01", "2026-10-01", True),
        ("2020-01-01", "2026-10-01", True),
        ("2026-10-01", "2026-10-02", False),
        ("2026-10-02", "2026-10-03", False),
    ],
)
def test_pull_requires_acknowledged_forward_coverage_for_each_account(
    protocol: str, start: str, end: str, complete: bool
) -> None:
    result = _completed_pull(protocol)
    source = result.source_results[0]
    assert isinstance(source, SimpleFINSourceResult)
    assert source.history is not None
    public_history = result.sources[0].history
    assert public_history is not None
    interval = UTCWindow.model_validate({
        "start": f"{start}T00:00:00Z",
        "end": f"{end}T00:00:00Z",
    })
    source.history.accounts[0].acknowledged_intervals = [interval]
    public_history.accounts[0].acknowledged_intervals = [interval.model_copy()]

    assert result.complete is complete


@pytest.mark.parametrize("protocol", ["v1", "v2"])
def test_pull_accepts_incidental_older_windows_outside_account_credit(
    protocol: str,
) -> None:
    result = _completed_pull(protocol)
    source = result.source_results[0]
    additional = source.outcomes[0].model_copy(deep=True)
    additional.window = UTCWindow.model_validate({
        "start": "2026-08-01T00:00:00Z",
        "end": "2026-09-01T00:00:00Z",
    })
    source.outcomes.append(additional)

    assert result.complete


@pytest.mark.parametrize("protocol", ["v1", "v2"])
def test_pull_rejects_history_gap_at_retrieval_frontier(protocol: str) -> None:
    result = _completed_pull(protocol)
    source = result.source_results[0]
    assert isinstance(source, SimpleFINSourceResult)
    assert source.history is not None
    public_history = result.sources[0].history
    assert public_history is not None
    intervals = [
        UTCWindow.model_validate({"start": start, "end": end})
        for start, end in [
            ("2026-09-01T00:00:00Z", "2026-09-30T00:00:00Z"),
            ("2026-10-02T00:00:00Z", "2026-10-03T00:00:00Z"),
        ]
    ]
    source.history.accounts[0].acknowledged_intervals = intervals
    public_history.accounts[0].acknowledged_intervals = intervals

    assert not result.complete


@pytest.mark.parametrize("protocol", ["v1", "v2"])
def test_pull_uses_latest_complete_window_for_each_native_scope(protocol: str) -> None:
    result = _completed_pull(protocol)
    source = result.source_results[0]
    assert isinstance(source, SimpleFINSourceResult)
    assert source.history is not None
    public_history = result.sources[0].history
    assert public_history is not None
    later = source.outcomes[0].model_copy(deep=True)
    source.outcomes.append(later)
    source.outcomes[0].window = UTCWindow.model_validate({
        "start": "2026-09-01T00:00:00Z",
        "end": "2026-09-10T00:00:00Z",
    })
    interval = UTCWindow.model_validate({
        "start": "2026-09-01T00:00:00Z",
        "end": "2026-09-20T00:00:00Z",
    })
    source.history.accounts[0].acknowledged_intervals = [interval]
    public_history.accounts[0].acknowledged_intervals = [interval.model_copy()]

    assert not result.complete


@pytest.mark.parametrize("protocol", ["v1", "v2"])
def test_historical_pull_uses_its_requested_window_instead_of_today(
    protocol: str,
) -> None:
    result = _completed_pull(protocol)
    source = result.source_results[0]
    assert isinstance(source, SimpleFINSourceResult)
    assert source.history is not None
    public_history = result.sources[0].history
    assert public_history is not None
    historical = UTCWindow.model_validate({
        "start": "2020-01-01T00:00:00Z",
        "end": "2020-02-01T00:00:00Z",
    })
    for outcome in source.outcomes:
        outcome.window = historical.model_copy()
    for account in source.history.accounts:
        account.acknowledged_intervals = [historical.model_copy()]
    for account in public_history.accounts:
        account.acknowledged_intervals = [historical.model_copy()]

    assert result.complete


@pytest.mark.parametrize("evidence", ["no_sources", "no_outcomes", "mixed"])
def test_pull_incomplete_without_source_retrieval_evidence(evidence: str) -> None:
    result = _completed_pull("v2")
    empty = PlaidSourceResult.model_validate({
        "provider": "plaid",
        "connection_id": "22222222-2222-4222-8222-222222222222",
        "credential_generation": 1,
        "outcomes": [],
    })
    if evidence == "mixed":
        result.source_results.append(empty)
    else:
        result.sources = []
        result.source_results = [] if evidence == "no_sources" else [empty]

    assert not result.complete


@pytest.mark.parametrize("account_id", ["plaid-account-1", None])
def test_pull_complete_with_zero_transactions_and_plaid_retrieval_evidence(
    account_id: str | None,
) -> None:
    result = _completed_pull("v2")
    envelope = json.loads((FIXTURES / "plaid.json").read_text())
    source = envelope["sources"][0]
    del source["batches"]
    source["outcomes"][0]["account_id"] = account_id
    result.sources = []
    result.source_results = [PlaidSourceResult.model_validate(source)]

    assert result.transactions_loaded == 0
    assert result.complete
