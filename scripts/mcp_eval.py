"""Load and score provider-neutral MCP surface evaluation evidence."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Literal, cast

from mcp.types import Tool

from moneybin.mcp.surface_inventory import SurfaceInventory

_SURFACE_IDS = ("baseline-105", "standard-47")
_EVIDENCE_KINDS = ("contract_fixture", "observed")
_FIXTURE_TOKEN_METHOD = "deterministic_estimate:ceil(canonical_registry_utf8_bytes/4)"  # noqa: S105  # Metadata accounting label, not a credential.
_DEFAULT_INVENTORY = (
    Path(__file__).parents[1] / "tests/fixtures/mcp_surface/baseline-2026-07-17.json"
)
_DEFAULT_BASELINE_INVENTORY = _DEFAULT_INVENTORY
_DEFAULT_CANDIDATE_INVENTORY = (
    Path(__file__).parents[1] / "tests/fixtures/mcp_surface/standard-47.json"
)
_CONTEXT_BUDGET_LIMIT = 0.02

JSONValue = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]


@dataclass(frozen=True, slots=True)
class ToolCall:
    """One ordered MCP tool invocation."""

    name: str
    arguments: Mapping[str, JSONValue]


@dataclass(frozen=True, slots=True)
class EvalExpectation:
    """Expected calls and terminal state for one surface."""

    calls: tuple[ToolCall, ...]
    completed: bool
    safety_outcome: str
    recovered: bool


@dataclass(frozen=True, slots=True)
class EvalCase:
    """One provider-neutral prompt with surface-specific expectations."""

    id: str
    prompt: str
    expectations: Mapping[str, EvalExpectation]
    workflow: tuple[str, ...]
    safety: tuple[str, ...]

    def expectation_for(self, surface_id: str) -> EvalExpectation:
        """Return this case's expectation for a registered surface."""
        try:
            return self.expectations[surface_id]
        except KeyError as error:
            raise ValueError(
                f"case {self.id!r} has no expectation for surface {surface_id!r}"
            ) from error


@dataclass(frozen=True, slots=True)
class EvalResponse:
    """Captured response state for one evaluation case."""

    case_id: str
    calls: tuple[ToolCall, ...]
    completed: bool
    safety_outcome: str
    recovered: bool


@dataclass(frozen=True, slots=True)
class EvalCapture:
    """Provider-neutral evidence from one surface evaluation run."""

    surface_id: str
    host: str
    model: str
    run_date: date
    evidence_kind: Literal["contract_fixture", "observed"]
    registry_sha256: str
    metadata_bytes: int
    metadata_tokens: int
    metadata_token_method: str
    context_window_tokens: int | None
    responses: tuple[EvalResponse, ...]


@dataclass(frozen=True, slots=True)
class CaseOutcome:
    """Deterministic scores for one case."""

    case_id: str
    selection: bool
    arguments: bool
    workflow: bool
    safety: bool
    unnecessary_calls: int
    recovery: bool

    @property
    def passed(self) -> bool:
        """Return whether every expected dimension passed."""
        return (
            self.selection
            and self.arguments
            and self.workflow
            and self.safety
            and self.unnecessary_calls == 0
            and self.recovery
        )


@dataclass(frozen=True, slots=True)
class EvalResult:
    """Aggregate and per-case deterministic evaluation scores."""

    surface_id: str
    metadata_bytes: int
    selection: float
    arguments: float
    workflow: float
    safety: float
    unnecessary_calls: int
    recovery: float
    outcomes: tuple[CaseOutcome, ...]

    @classmethod
    def from_outcomes(
        cls,
        outcomes: tuple[CaseOutcome, ...],
        *,
        surface_id: str,
        metadata_bytes: int,
    ) -> EvalResult:
        """Aggregate case outcomes into normalized dimension scores."""
        if not outcomes:
            raise ValueError("evaluation corpus must contain at least one case")
        count = len(outcomes)
        return cls(
            surface_id=surface_id,
            metadata_bytes=metadata_bytes,
            selection=sum(item.selection for item in outcomes) / count,
            arguments=sum(item.arguments for item in outcomes) / count,
            workflow=sum(item.workflow for item in outcomes) / count,
            safety=sum(item.safety for item in outcomes) / count,
            unnecessary_calls=sum(item.unnecessary_calls for item in outcomes),
            recovery=sum(item.recovery for item in outcomes) / count,
            outcomes=outcomes,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the JSON-ready score summary."""
        return {
            "surface_id": self.surface_id,
            "metadata_bytes": self.metadata_bytes,
            "selection": self.selection,
            "arguments": self.arguments,
            "workflow": self.workflow,
            "safety": self.safety,
            "unnecessary_calls": self.unnecessary_calls,
            "recovery": self.recovery,
            "outcomes": [
                {**asdict(outcome), "passed": outcome.passed}
                for outcome in self.outcomes
            ],
        }


@dataclass(frozen=True, slots=True)
class EvalComparison:
    """Candidate deltas and acceptance gates relative to a baseline."""

    selection_delta: float
    argument_delta: float
    workflow_delta: float
    safety_delta: float
    unnecessary_calls_delta: int
    recovery_delta: float
    metadata_bytes_delta: int

    @property
    def failed_gates(self) -> tuple[str, ...]:
        """Return every regressed acceptance dimension."""
        failures: list[str] = []
        if self.selection_delta < 0:
            failures.append("selection")
        if self.argument_delta < 0:
            failures.append("arguments")
        if self.workflow_delta < 0:
            failures.append("workflow")
        if self.safety_delta < 0:
            failures.append("safety")
        if self.unnecessary_calls_delta > 0:
            failures.append("unnecessary_calls")
        if self.recovery_delta < 0:
            failures.append("recovery")
        if self.metadata_bytes_delta >= 0:
            failures.append("metadata_bytes")
        return tuple(failures)

    @property
    def passed(self) -> bool:
        """Return whether the candidate satisfies every comparison gate."""
        return not self.failed_gates

    def to_dict(self) -> dict[str, object]:
        """Return the JSON-ready comparison."""
        return {
            **asdict(self),
            "passed": self.passed,
            "failed_gates": list(self.failed_gates),
        }


def compare(baseline: EvalResult, candidate: EvalResult) -> EvalComparison:
    """Compare candidate quality and metadata with a baseline."""
    return EvalComparison(
        selection_delta=candidate.selection - baseline.selection,
        argument_delta=candidate.arguments - baseline.arguments,
        workflow_delta=candidate.workflow - baseline.workflow,
        safety_delta=candidate.safety - baseline.safety,
        unnecessary_calls_delta=(
            candidate.unnecessary_calls - baseline.unnecessary_calls
        ),
        recovery_delta=candidate.recovery - baseline.recovery,
        metadata_bytes_delta=candidate.metadata_bytes - baseline.metadata_bytes,
    )


def load_cases(path: Path) -> tuple[EvalCase, ...]:
    """Load a strict JSON evaluation corpus."""
    payload = _array(_read_json(path), "case corpus")
    if not payload:
        raise ValueError("case corpus must be a non-empty JSON array")
    cases = tuple(_parse_case(item, index) for index, item in enumerate(payload))
    ids = [case.id for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("case corpus contains duplicate case IDs")
    return cases


def load_capture(path: Path) -> EvalCapture:
    """Load one strict JSON capture."""
    payload = _object(_read_json(path), "capture")
    _reject_unknown(
        payload,
        {
            "surface_id",
            "host",
            "model",
            "run_date",
            "evidence_kind",
            "registry_sha256",
            "metadata_bytes",
            "metadata_tokens",
            "metadata_token_method",
            "context_window_tokens",
            "responses",
        },
        "capture",
    )
    surface_id = _nonempty_string(payload, "surface_id", "capture")
    if surface_id not in _SURFACE_IDS:
        raise ValueError(f"capture surface_id must be one of {_SURFACE_IDS}")
    evidence_kind = _nonempty_string(payload, "evidence_kind", "capture")
    if evidence_kind not in _EVIDENCE_KINDS:
        raise ValueError(f"capture evidence_kind must be one of {_EVIDENCE_KINDS}")
    run_date_text = _nonempty_string(payload, "run_date", "capture")
    try:
        run_date = date.fromisoformat(run_date_text)
    except ValueError as error:
        raise ValueError("capture run_date must be an ISO date") from error
    responses_payload = _array(
        _required(payload, "responses", "capture"),
        "capture responses",
    )
    capture = EvalCapture(
        surface_id=surface_id,
        host=_nonempty_string(payload, "host", "capture"),
        model=_nonempty_string(payload, "model", "capture"),
        run_date=run_date,
        evidence_kind=evidence_kind,
        registry_sha256=_nonempty_string(payload, "registry_sha256", "capture"),
        metadata_bytes=_positive_int(payload, "metadata_bytes", "capture"),
        metadata_tokens=_positive_int(payload, "metadata_tokens", "capture"),
        metadata_token_method=_nonempty_string(
            payload, "metadata_token_method", "capture"
        ),
        context_window_tokens=_nullable_positive_int(
            payload, "context_window_tokens", "capture"
        ),
        responses=tuple(
            _parse_response(item, index) for index, item in enumerate(responses_payload)
        ),
    )
    _validate_evidence_labels(capture)
    return capture


def score(
    cases: Sequence[EvalCase],
    capture: EvalCapture,
    inventory: SurfaceInventory,
) -> EvalResult:
    """Score a capture against its corpus and exact registry inventory."""
    if capture.registry_sha256 != inventory.sha256:
        raise ValueError("capture registry_sha256 does not match inventory")
    if capture.metadata_bytes != inventory.total_bytes:
        raise ValueError("capture metadata_bytes does not match inventory")
    case_ids = [case.id for case in cases]
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("corpus contains duplicate case IDs")
    response_ids = [response.case_id for response in capture.responses]
    if len(response_ids) != len(set(response_ids)):
        raise ValueError("capture contains duplicate response case IDs")
    by_id = {response.case_id: response for response in capture.responses}
    if set(by_id) != set(case_ids):
        raise ValueError("capture case IDs do not match corpus")
    outcomes = tuple(
        _score_case(
            case.id,
            case.expectation_for(capture.surface_id),
            by_id[case.id],
        )
        for case in cases
    )
    return EvalResult.from_outcomes(
        outcomes,
        surface_id=capture.surface_id,
        metadata_bytes=capture.metadata_bytes,
    )


def _score_case(
    case_id: str,
    expectation: EvalExpectation,
    response: EvalResponse,
) -> CaseOutcome:
    expected_names = tuple(call.name for call in expectation.calls)
    actual_names = tuple(call.name for call in response.calls)
    selection = actual_names == expected_names
    arguments = len(response.calls) == len(expectation.calls) and all(
        _canonical_json(actual.arguments) == _canonical_json(expected.arguments)
        for expected, actual in zip(expectation.calls, response.calls, strict=True)
    )
    unnecessary = sum((Counter(actual_names) - Counter(expected_names)).values())
    return CaseOutcome(
        case_id=case_id,
        selection=selection,
        arguments=arguments,
        workflow=response.completed == expectation.completed,
        safety=response.safety_outcome == expectation.safety_outcome,
        unnecessary_calls=unnecessary,
        recovery=response.recovered == expectation.recovered,
    )


def _parse_case(value: object, index: int) -> EvalCase:
    context = f"case[{index}]"
    payload = _object(value, context)
    _reject_unknown(
        payload,
        {"id", "prompt", "expectations", "workflow", "safety"},
        context,
    )
    expectations_payload = _object(
        _required(payload, "expectations", context),
        f"{context}.expectations",
    )
    if set(expectations_payload) != set(_SURFACE_IDS):
        raise ValueError(f"{context}.expectations must contain {_SURFACE_IDS}")
    expectations = {
        surface_id: _parse_expectation(
            expectations_payload[surface_id],
            f"{context}.expectations.{surface_id}",
        )
        for surface_id in _SURFACE_IDS
    }
    return EvalCase(
        id=_nonempty_string(payload, "id", context),
        prompt=_nonempty_string(payload, "prompt", context),
        expectations=expectations,
        workflow=_string_tuple(payload, "workflow", context),
        safety=_string_tuple(payload, "safety", context),
    )


def _parse_expectation(value: object, context: str) -> EvalExpectation:
    payload = _object(value, context)
    _reject_unknown(
        payload,
        {"calls", "completed", "safety_outcome", "recovered"},
        context,
    )
    return EvalExpectation(
        calls=_parse_calls(payload, context),
        completed=_boolean(payload, "completed", context),
        safety_outcome=_nonempty_string(payload, "safety_outcome", context),
        recovered=_boolean(payload, "recovered", context),
    )


def _parse_response(value: object, index: int) -> EvalResponse:
    context = f"capture.responses[{index}]"
    payload = _object(value, context)
    _reject_unknown(
        payload,
        {"case_id", "calls", "completed", "safety_outcome", "recovered"},
        context,
    )
    return EvalResponse(
        case_id=_nonempty_string(payload, "case_id", context),
        calls=_parse_calls(payload, context),
        completed=_boolean(payload, "completed", context),
        safety_outcome=_nonempty_string(payload, "safety_outcome", context),
        recovered=_boolean(payload, "recovered", context),
    )


def _parse_calls(payload: Mapping[str, object], context: str) -> tuple[ToolCall, ...]:
    raw_calls = _array(_required(payload, "calls", context), f"{context}.calls")
    calls: list[ToolCall] = []
    for index, value in enumerate(raw_calls):
        call_context = f"{context}.calls[{index}]"
        call = _object(value, call_context)
        _reject_unknown(call, {"name", "arguments"}, call_context)
        arguments = _object(
            _required(call, "arguments", call_context),
            f"{call_context}.arguments",
        )
        calls.append(
            ToolCall(
                name=_nonempty_string(call, "name", call_context),
                arguments=cast(
                    "Mapping[str, JSONValue]",
                    arguments,
                ),  # JSON decoding guarantees JSON-compatible values.
            )
        )
    return tuple(calls)


def _validate_evidence_labels(capture: EvalCapture) -> None:
    if capture.evidence_kind == "contract_fixture":
        if capture.host != "contract-fixture" or capture.model != "deterministic":
            raise ValueError(
                "contract_fixture requires host='contract-fixture' and "
                "model='deterministic'"
            )
        if capture.metadata_token_method != _FIXTURE_TOKEN_METHOD:
            raise ValueError(
                f"contract_fixture metadata_token_method must be {_FIXTURE_TOKEN_METHOD!r}"
            )
        expected_tokens = (capture.metadata_bytes + 3) // 4
        if capture.metadata_tokens != expected_tokens:
            raise ValueError(
                "contract_fixture metadata_tokens must equal ceil(metadata_bytes/4)"
            )
        if capture.context_window_tokens is not None:
            raise ValueError(
                "contract_fixture context_window_tokens must be null; "
                "only observed evidence may claim a context window"
            )
    else:
        if not capture.metadata_token_method.startswith("host_reported"):
            raise ValueError(
                "observed metadata_token_method must identify host_reported accounting"
            )
        if capture.context_window_tokens is None:
            raise ValueError(
                "observed evidence requires documented context_window_tokens"
            )


def _read_json(path: Path) -> object:
    try:
        return cast(
            object,
            json.loads(
                path.read_text(),
                object_pairs_hook=_strict_json_object,
                parse_constant=_reject_non_finite_constant,
            ),
        )
    except (OSError, ValueError) as error:
        raise ValueError(f"could not load strict JSON from {path}: {error}") from error


def _strict_json_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object member {key!r}")
        result[key] = value
    return result


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is not allowed: {value}")


def _object(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{context} must be a JSON object")
    return cast("dict[str, object]", value)


def _array(value: object, context: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be a JSON array")
    return cast("list[object]", value)


def _reject_unknown(
    payload: Mapping[str, object],
    expected: set[str],
    context: str,
) -> None:
    unknown = set(payload) - expected
    if unknown:
        raise ValueError(f"{context} has unknown fields: {sorted(unknown)}")


def _required(payload: Mapping[str, object], field: str, context: str) -> object:
    try:
        return payload[field]
    except KeyError as error:
        raise ValueError(f"{context} is missing required field {field}") from error


def _nonempty_string(
    payload: Mapping[str, object],
    field: str,
    context: str,
) -> str:
    value = _required(payload, field, context)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}.{field} must be a non-empty string")
    return value


def _positive_int(
    payload: Mapping[str, object],
    field: str,
    context: str,
) -> int:
    value = _required(payload, field, context)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{context}.{field} must be a positive integer")
    return value


def _nullable_positive_int(
    payload: Mapping[str, object],
    field: str,
    context: str,
) -> int | None:
    value = _required(payload, field, context)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{context}.{field} must be null or a positive integer")
    return value


def _boolean(
    payload: Mapping[str, object],
    field: str,
    context: str,
) -> bool:
    value = _required(payload, field, context)
    if not isinstance(value, bool):
        raise ValueError(f"{context}.{field} must be a boolean")
    return value


def _string_tuple(
    payload: Mapping[str, object],
    field: str,
    context: str,
) -> tuple[str, ...]:
    value = _array(_required(payload, field, context), f"{context}.{field}")
    if not value or not all(isinstance(item, str) and item.strip() for item in value):
        raise ValueError(f"{context}.{field} must be a non-empty string array")
    strings = cast("list[str]", value)
    if len(strings) != len(set(strings)):
        raise ValueError(f"{context}.{field} must not contain duplicates")
    return tuple(strings)


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except ValueError as error:
        raise ValueError("canonical JSON cannot serialize non-finite values") from error


def _load_inventory(path: Path) -> SurfaceInventory:
    payload = _object(_read_json(path), "inventory")
    tools = _array(_required(payload, "tools", "inventory"), "inventory.tools")
    try:
        definitions = [
            Tool.model_validate(_object(row, "inventory tool")["definition"])
            for row in tools
        ]
    except (KeyError, ValueError) as error:
        raise ValueError(f"invalid inventory tool definition: {error}") from error
    return SurfaceInventory.from_tools(definitions)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    score_parser = subparsers.add_parser("score")
    score_parser.add_argument("--cases", required=True, type=Path)
    score_parser.add_argument("--capture", required=True, type=Path)
    score_parser.add_argument("--inventory", type=Path, default=_DEFAULT_INVENTORY)
    score_parser.add_argument("--require-observed", action="store_true")
    compare_parser = subparsers.add_parser("compare")
    compare_parser.add_argument("--cases", required=True, type=Path)
    compare_parser.add_argument("--baseline", required=True, type=Path)
    compare_parser.add_argument(
        "--baseline-inventory",
        type=Path,
        default=_DEFAULT_BASELINE_INVENTORY,
    )
    compare_parser.add_argument("--candidate", required=True, type=Path)
    compare_parser.add_argument(
        "--candidate-inventory",
        type=Path,
        default=_DEFAULT_CANDIDATE_INVENTORY,
    )
    compare_parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the evaluator CLI."""
    args = _parser().parse_args(argv)
    try:
        if args.command == "score":
            cases = load_cases(args.cases)
            capture = load_capture(args.capture)
            if args.require_observed and capture.evidence_kind != "observed":
                raise ValueError("contract_fixture is not observed evidence")
            result = score(cases, capture, _load_inventory(args.inventory))
            sys.stdout.write(
                json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n"
            )
            return 0
        return _compare_command(args)
    except ValueError as error:
        sys.stderr.write(f"{error}\n")
        return 1


def _compare_command(args: argparse.Namespace) -> int:
    cases = load_cases(args.cases)
    baseline_capture = load_capture(args.baseline)
    candidate_capture = load_capture(args.candidate)
    if (baseline_capture.host, baseline_capture.model) != (
        candidate_capture.host,
        candidate_capture.model,
    ):
        raise ValueError("baseline and candidate must use the same host and model")
    if baseline_capture.evidence_kind != candidate_capture.evidence_kind:
        raise ValueError("baseline and candidate must use the same evidence_kind")
    baseline = score(
        cases,
        baseline_capture,
        _load_inventory(args.baseline_inventory),
    )
    candidate = score(
        cases,
        candidate_capture,
        _load_inventory(args.candidate_inventory),
    )
    comparison = compare(baseline, candidate)
    context_budget = _context_budget(candidate_capture)
    host_native_deferral = {"status": "not_observed"}
    command_succeeded = comparison.passed and context_budget["status"] != "failed"
    promotion_ready = (
        comparison.passed
        and context_budget["status"] == "passed"
        and host_native_deferral["status"] == "passed"
    )
    payload = {
        "baseline": baseline.to_dict(),
        "candidate": candidate.to_dict(),
        "comparison": comparison.to_dict(),
        "contract_passed": comparison.passed,
        "evidence": {
            "evidence_kind": candidate_capture.evidence_kind,
            "host": candidate_capture.host,
            "model": candidate_capture.model,
            "baseline_run_date": baseline_capture.run_date.isoformat(),
            "candidate_run_date": candidate_capture.run_date.isoformat(),
            "baseline_registry_sha256": baseline_capture.registry_sha256,
            "candidate_registry_sha256": candidate_capture.registry_sha256,
            "same_host_model": True,
            "context_budget": context_budget,
            "host_native_deferral": host_native_deferral,
        },
        "promotion_ready": promotion_ready,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
    )
    return 0 if command_succeeded else 1


def _context_budget(capture: EvalCapture) -> dict[str, object]:
    if capture.context_window_tokens is None:
        return {
            "limit": _CONTEXT_BUDGET_LIMIT,
            "ratio": None,
            "status": "not_observed",
        }
    ratio = capture.metadata_tokens / capture.context_window_tokens
    return {
        "limit": _CONTEXT_BUDGET_LIMIT,
        "ratio": ratio,
        "status": "passed" if ratio < _CONTEXT_BUDGET_LIMIT else "failed",
    }


if __name__ == "__main__":
    raise SystemExit(main())
