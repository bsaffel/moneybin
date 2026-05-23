"""Tier-derivation tests for the privacy/consent typed payloads."""

from moneybin.privacy.introspection import derive_tier
from moneybin.privacy.taxonomy import Tier


def test_status_payload_is_low_tier():
    from moneybin.privacy.payloads.consent import PrivacyStatusPayload

    # No HIGH/MEDIUM fields — status is operational metadata, not financial data.
    assert derive_tier(PrivacyStatusPayload) == Tier.LOW


def test_mutation_and_log_payloads_are_low_tier():
    from moneybin.privacy.payloads.consent import (
        ConsentMutationPayload,
        ConsentRevokeAllPayload,
        PrivacyLogPayload,
    )

    assert derive_tier(ConsentMutationPayload) == Tier.LOW
    assert derive_tier(PrivacyLogPayload) == Tier.LOW
    assert derive_tier(ConsentRevokeAllPayload) == Tier.LOW
