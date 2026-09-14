"""Versioned dependencies for investment relationships and candidate graphs."""

from collections.abc import Mapping, Sequence
from typing import Any

from moneybin.investments.observation_versions import observation_version

ALGORITHM_VERSION = "investment_match_v1"

_LEG_DEPENDENCIES = (
    "source_event_key",
    "source_type",
    "source_origin",
    "native_reference",
    "observation_version",
    "leg_role",
    "type",
    "subtype",
    "account_id",
    "security_id",
    "account_identity_generation",
    "security_identity_generation",
    "source_currency_code",
    "currency_code",
    "trade_date",
    "trade_date_basis",
    "settlement_date",
    "original_acquisition_date",
    "quantity",
    "price",
    "amount",
    "fees",
)


def leg_dependencies(leg: Mapping[str, Any]) -> dict[str, Any]:
    """Bind canonical currency only when it supplied the effective value."""
    result = {field: leg.get(field) for field in _LEG_DEPENDENCIES}
    if leg.get("source_currency_code") is None:
        result["account_currency_code"] = leg.get("account_currency_code")
    return result


def fingerprint(kind: str, content: object) -> str:
    """Use the repository's canonical decimal/date content-hash encoding."""
    return observation_version(
        kind, {"algorithm": ALGORITHM_VERSION, "content": content}
    )


def relationship_fingerprint(
    members: Sequence[str],
    legs: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
    supersession: Sequence[Mapping[str, Any]] = (),
) -> str:
    """Exclude unrelated alternatives, while binding complete supersession."""
    selected = set(members)
    return fingerprint(
        "investment_relationship",
        {
            "members": sorted(selected),
            "legs": sorted(
                [
                    leg_dependencies(leg)
                    for leg in legs
                    if leg["source_event_key"] in selected
                ],
                key=lambda row: (row["source_event_key"], row["native_reference"]),
            ),
            "evidence": sorted(
                [
                    dict(row)
                    for row in evidence
                    if row["left_source_event_key"] in selected
                    and row["right_source_event_key"] in selected
                ],
                key=lambda row: (
                    row["left_source_event_key"],
                    row["right_source_event_key"],
                    row["leg_role"],
                    row["left_native_reference"],
                ),
            ),
            "supersession": sorted(supersession, key=lambda row: row["decision_id"]),
        },
    )
