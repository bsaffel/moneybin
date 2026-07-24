"""Wire-shape guards for ResponseEnvelope.

These exist because `to_dict()` was for a long time a SECOND, divergent
representation of the envelope: FastMCP serializes the returned dataclass
directly and never calls `to_dict()`, so anything computed only inside
`to_dict()` (status) or held in a non-pydantic type (error) was silently
dropped on the wire while the test suite stayed green.
"""

from typing import Any, cast

import pytest
from pydantic import TypeAdapter

from moneybin import error_codes
from moneybin.errors import ErrorDetail, UserError
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    SummaryMeta,
    build_error_envelope,
)

_ADAPTER = TypeAdapter(ResponseEnvelope[Any])


def _dump(envelope: ResponseEnvelope[Any]) -> dict[str, Any]:
    """Serialize the way FastMCP does — via pydantic, not via to_dict()."""
    return cast(dict[str, Any], _ADAPTER.dump_python(envelope, mode="json"))


def test_error_code_and_hint_survive_pydantic_serialization() -> None:
    """The agent-facing fields must survive the path FastMCP actually takes."""
    envelope = build_error_envelope(
        error=UserError(
            "boom", code=error_codes.INFRA_IO_ERROR, hint="do X", details={"k": "v"}
        )
    )
    dumped = _dump(envelope)
    assert dumped["status"] == "error"
    assert dumped["error"]["message"] == "boom"
    assert dumped["error"]["code"] == error_codes.INFRA_IO_ERROR
    assert dumped["error"]["hint"] == "do X"
    assert dumped["error"]["details"] == {"k": "v"}


def test_success_envelope_reports_ok_status() -> None:
    envelope = ResponseEnvelope(
        summary=SummaryMeta(total_count=1, returned_count=1), data=[{"a": 1}]
    )
    assert _dump(envelope)["status"] == "ok"


def test_status_is_derived_not_caller_supplied() -> None:
    """A caller-supplied status is overwritten so it cannot contradict `error`."""
    lying = ResponseEnvelope(
        summary=SummaryMeta(total_count=0, returned_count=0),
        data=[],
        status="error",  # no error field set — must be corrected to "ok"
    )
    assert lying.status == "ok"
    assert _dump(lying)["status"] == "ok"

    errored = build_error_envelope(
        error=UserError("x", code=error_codes.INFRA_IO_ERROR)
    )
    assert errored.status == "error"


@pytest.mark.parametrize(
    "envelope",
    [
        ResponseEnvelope(summary=SummaryMeta(total_count=0, returned_count=0), data=[]),
        build_error_envelope(
            error=UserError("boom", code=error_codes.INFRA_IO_ERROR, hint="h")
        ),
    ],
    ids=["success", "error"],
)
def test_to_dict_matches_pydantic_dump(envelope: ResponseEnvelope[Any]) -> None:
    """to_dict() is a projection of the dataclass, never a divergent shape.

    This is the regression guard for the original defect: if someone adds a
    field to to_dict() that isn't a real dataclass field, this fails. The
    reverse direction is deliberately not asserted — to_dict() omits absent
    optionals (and a False `degraded`) that the raw dump still emits, which is
    a narrowing, not a divergence.
    """
    _assert_projection(envelope.to_dict(), _dump(envelope))


def _assert_projection(
    projected: dict[str, Any], dumped: dict[str, Any], path: str = ""
) -> None:
    """Assert every key/value `to_dict()` emits is backed by a real field."""
    for key, value in projected.items():
        assert key in dumped, (
            f"to_dict() emits {path}{key!r}, which no dataclass field backs — "
            "it would never reach the wire via FastMCP"
        )
        if isinstance(value, dict) and isinstance(dumped[key], dict):
            _assert_projection(
                cast(dict[str, Any], value),
                cast(dict[str, Any], dumped[key]),
                f"{path}{key}.",
            )
        else:
            assert dumped[key] == value, f"{path}{key} diverges from the dataclass"


def test_with_error_serializes_and_keeps_the_payload() -> None:
    """The regression: attaching an error to a payload-carrying envelope.

    `dataclasses.replace(envelope, error=UserError(...))` type-checks (replace
    is `**changes: Any`) but raises AttributeError inside `to_dict()`, because
    only `ErrorDetail` has `model_dump`. That turned `--output json` partial
    failures into empty stdout. `with_error` is typed, so the wrong call is a
    pyright error instead of a runtime one.
    """
    envelope = ResponseEnvelope(
        summary=SummaryMeta(total_count=2, returned_count=2),
        data={"error_details": [{"transaction_id": "t1", "reason": "bad id"}]},
    )
    failed = envelope.with_error(
        ErrorDetail(message="1 item(s) failed", code="categorization_errors")
    )

    assert failed.status == "error"
    dumped = failed.to_dict()
    assert dumped["status"] == "error"
    assert dumped["error"]["code"] == "categorization_errors"
    # Payload-carrying error envelopes keep their data — that is the whole
    # reason these call sites don't use build_error_envelope.
    assert dumped["data"]["error_details"][0]["transaction_id"] == "t1"
    _assert_projection(dumped, _dump(failed))


def test_with_error_leaves_the_original_untouched() -> None:
    """It returns a copy; `status` is re-derived rather than mutated in place."""
    envelope = ResponseEnvelope(
        summary=SummaryMeta(total_count=0, returned_count=0), data=[]
    )
    failed = envelope.with_error(
        ErrorDetail(message="boom", code=error_codes.INFRA_IO_ERROR)
    )

    assert envelope.status == "ok"
    assert envelope.error is None
    assert failed.status == "error"
