"""ResponseEnvelope + build_error_envelope plumbing for recovery_actions."""

from __future__ import annotations

from moneybin import error_codes
from moneybin.errors import RecoveryAction, UserError
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    SummaryMeta,
    build_error_envelope,
)


def _sample_action() -> RecoveryAction:
    return RecoveryAction(
        tool="system_audit_undo",
        arguments={"operation_id": "op_test"},
        rationale="Restore pre-mutation state",
        confidence="certain",
        idempotent=True,
    )


class TestResponseEnvelopeRecoveryActions:
    """Test recovery_actions field on ResponseEnvelope."""

    def test_envelope_default_recovery_actions_none(self):
        summary = SummaryMeta(
            total_count=0,
            returned_count=0,
            has_more=False,
            sensitivity="low",
            display_currency="USD",
            degraded=False,
        )
        env = ResponseEnvelope(summary=summary, data=[])
        assert env.recovery_actions is None

    def test_envelope_accepts_recovery_actions(self):
        summary = SummaryMeta(
            total_count=0,
            returned_count=0,
            has_more=False,
            sensitivity="low",
            display_currency="USD",
            degraded=False,
        )
        env = ResponseEnvelope(
            summary=summary,
            data=[],
            recovery_actions=[_sample_action()],
        )
        assert env.recovery_actions is not None
        assert len(env.recovery_actions) == 1


class TestBuildErrorEnvelope:
    """Test build_error_envelope threading of recovery_actions."""

    def test_error_envelope_threads_recovery_actions_from_user_error(self):
        err = UserError(
            "Boom",
            code=error_codes.MUTATION_NOT_FOUND,
            recovery_actions=[_sample_action()],
        )
        env = build_error_envelope(error=err, sensitivity="low")
        assert env.error is err
        assert env.recovery_actions is not None
        assert env.recovery_actions[0].tool == "system_audit_undo"

    def test_error_envelope_passes_explicit_recovery_actions(self):
        """Explicit kwarg overrides any actions on the UserError."""
        err = UserError("Boom", code=error_codes.MUTATION_NOT_FOUND)
        override = [
            RecoveryAction(
                tool="system_doctor",
                arguments={},
                rationale="Re-run doctor",
                confidence="suggested",
                idempotent=True,
            ),
        ]
        env = build_error_envelope(
            error=err,
            sensitivity="low",
            recovery_actions=override,
        )
        assert env.recovery_actions == override

    def test_error_envelope_explicit_empty_overrides_non_empty_error_actions(self):
        """Passing `[]` explicitly suppresses the error's own actions.

        The most load-bearing precedence case: caller wants to clear
        recovery_actions for this surface even though the UserError carries
        some. Verifies `[]` is honored over the fall-back-to-error semantics.
        """
        err = UserError(
            "Boom",
            code=error_codes.MUTATION_NOT_FOUND,
            recovery_actions=[_sample_action()],
        )
        env = build_error_envelope(
            error=err,
            sensitivity="low",
            recovery_actions=[],
        )
        assert env.recovery_actions == []

    def test_direct_construction_with_error_actions_serializes_top_level(self):
        """Directly-built envelope still serializes the error's actions top-level.

        ResponseEnvelope(error=err) built directly (not via build_error_envelope)
        must still emit the error's recovery_actions at the top level. Regression
        guard: to_dict() strips recovery_actions from the nested error dict, so
        if the top-level field weren't backfilled from the error, the actions
        would vanish entirely from the wire for this already-used construction
        pattern.
        """
        err = UserError(
            "boom",
            code=error_codes.MUTATION_NOT_FOUND,
            recovery_actions=[_sample_action()],
        )
        env = ResponseEnvelope(
            summary=SummaryMeta(
                total_count=0,
                returned_count=0,
                has_more=False,
                sensitivity="low",
                display_currency="USD",
                degraded=False,
            ),
            data=[],
            error=err,
            # NOTE: recovery_actions NOT passed — left at default None
        )
        d = env.to_dict()
        assert d["recovery_actions"][0]["tool"] == "system_audit_undo"
        assert "recovery_actions" not in d["error"]

    def test_explicit_override_does_not_leak_via_nested_error_dict(self):
        """Suppression must take effect at the wire boundary, not just at env top-level.

        Regression guard for the redaction use case: caller passes
        recovery_actions=[] to redact actions for a low-trust surface, but
        the original UserError still carries the un-redacted actions. The
        envelope's to_dict() MUST NOT serialize them under d["error"] —
        otherwise the override leaks the unsuppressed actions.
        """
        err = UserError(
            "Boom",
            code=error_codes.MUTATION_NOT_FOUND,
            recovery_actions=[_sample_action()],
        )
        env = build_error_envelope(
            error=err,
            sensitivity="low",
            recovery_actions=[],
        )
        d = env.to_dict()
        assert d["recovery_actions"] == []
        # Nested error dict must NOT carry the suppressed actions:
        assert "recovery_actions" not in d["error"], (
            f"Nested error.recovery_actions leaks original actions: {d['error']!r}"
        )

    def test_error_envelope_empty_recovery_actions_preserved(self):
        """An explicit empty list reaches the envelope (not coerced to None)."""
        err = UserError(
            "Boom",
            code=error_codes.RECOVERY_NO_PATH,
            recovery_actions=[],
        )
        env = build_error_envelope(error=err, sensitivity="low")
        assert env.recovery_actions == []


class TestEnvelopeSerialization:
    """Test ResponseEnvelope.to_dict() serialization of recovery_actions."""

    def test_to_dict_serializes_recovery_actions(self):
        err = UserError(
            "boom",
            code=error_codes.MUTATION_NOT_FOUND,
            recovery_actions=[_sample_action()],
        )
        env = build_error_envelope(error=err, sensitivity="low")
        d = env.to_dict()
        assert "recovery_actions" in d
        assert d["recovery_actions"][0]["tool"] == "system_audit_undo"
        assert d["recovery_actions"][0]["confidence"] == "certain"

    def test_to_dict_omits_recovery_actions_when_none(self):
        env = ResponseEnvelope(
            summary=SummaryMeta(
                total_count=0,
                returned_count=0,
                has_more=False,
                sensitivity="low",
                display_currency="USD",
                degraded=False,
            ),
            data=[],
        )
        d = env.to_dict()
        assert "recovery_actions" not in d

    def test_to_dict_serializes_empty_recovery_actions(self):
        """Empty list = explicit 'nothing actionable'; must appear in output."""
        err = UserError(
            "boom",
            code=error_codes.RECOVERY_NO_PATH,
            recovery_actions=[],
        )
        env = build_error_envelope(error=err, sensitivity="low")
        d = env.to_dict()
        assert d["recovery_actions"] == []

    def test_to_dict_handles_plain_dict_recovery_actions(self):
        """Plain dicts (e.g., from deserialized JSON) pass through unchanged.

        Coercing defensively prevents a classified UserError from becoming
        an internal AttributeError at the wire boundary.
        """
        plain = {
            "tool": "system_audit_undo",
            "arguments": {"operation_id": "op_test"},
            "rationale": "Restore pre-mutation state",
            "confidence": "certain",
            "idempotent": True,
        }
        env = ResponseEnvelope(
            summary=SummaryMeta(
                total_count=0,
                returned_count=0,
                has_more=False,
                sensitivity="low",
                display_currency="USD",
                degraded=False,
            ),
            data=[],
            recovery_actions=[plain],  # type: ignore[list-item]
        )
        d = env.to_dict()
        assert d["recovery_actions"][0] == plain
