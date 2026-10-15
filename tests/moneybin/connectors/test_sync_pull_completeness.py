"""A successful receipt requires retrieval evidence for every known native account."""

import json
from pathlib import Path
from typing import Any

import pytest

from moneybin.connectors.sync_models import (
    PullResult,
    SimpleFINSourceResult,
    SimpleFINV1OrgScope,
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
