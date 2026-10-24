"""History snapshots keep acknowledged credit separate from pending intent."""

from copy import deepcopy
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from moneybin.connectors.sync_models import SimpleFINSource


def _source() -> dict[str, Any]:
    return {
        "provider": "simplefin",
        "connection_id": str(UUID(int=100)),
        "credential_generation": 1,
        "protocol_version": "2.0.0-draft",
        "batches": [],
        "outcomes": [],
        "history": {
            "observed_at": "2026-10-01T12:00:00Z",
            "replay_required": True,
            "discovery_history_target": "2026-01-01",
            "accounts": [
                {
                    "account_id": "a",
                    "native_scope": {
                        "kind": "simplefin_v2_connection",
                        "conn_id": "native",
                    },
                    "acknowledged_intervals": [
                        {
                            "start": "2026-01-01T00:00:00Z",
                            "end": "2026-05-01T00:00:00Z",
                        },
                        {
                            "start": "2026-09-01T00:00:00Z",
                            "end": "2026-10-01T00:00:00Z",
                        },
                    ],
                    "history_target": None,
                }
            ],
        },
    }


def test_cumulative_history_retains_gap_without_pending_target() -> None:
    """Credit may exceed one fetch window and a gap survives a null older target."""
    original = _source()
    result = SimpleFINSource.model_validate(original).model_dump(mode="json")
    assert result["history"] == original["history"]
    assert "complete" not in result["history"]


def test_empty_discovery_retains_pending_history_and_replay() -> None:
    """No discovered account must not erase a pending connection-wide request."""
    original = _source()
    original["history"]["accounts"] = []
    result = SimpleFINSource.model_validate(original).model_dump(mode="json")
    assert result["history"]["discovery_history_target"] == "2026-01-01"
    assert result["history"]["replay_required"] is True


@pytest.mark.parametrize(
    "invalid", ["midday", "overlap", "reversed", "three", "scope", "duplicate"]
)
def test_history_rejects_ambiguous_credit(invalid: str) -> None:
    """Invalid coverage must fail before any client uses it as acknowledged truth."""
    original = _source()
    account = original["history"]["accounts"][0]
    intervals = account["acknowledged_intervals"]
    if invalid == "midday":
        intervals[0]["start"] = "2026-01-01T12:00:00Z"
    elif invalid == "overlap":
        intervals[1]["start"] = "2026-04-01T00:00:00Z"
    elif invalid == "reversed":
        intervals.reverse()
    elif invalid == "three":
        intervals.append({
            "start": "2026-11-01T00:00:00Z",
            "end": "2026-12-01T00:00:00Z",
        })
    elif invalid == "scope":
        account["native_scope"] = {
            "kind": "simplefin_v1_org",
            "org": {"sfin-url": "https://synthetic.invalid", "name": "Synthetic"},
        }
    else:
        original["history"]["accounts"].append(deepcopy(account))
    with pytest.raises(ValidationError):
        SimpleFINSource.model_validate(original)
