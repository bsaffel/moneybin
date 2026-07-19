"""Contract tests for provider-neutral MCP surface evaluation evidence."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from mcp.types import Tool

from moneybin.mcp.surface_inventory import SurfaceInventory
from scripts.mcp_eval import (
    EvalResponse,
    ToolCall,
    compare,
    load_capture,
    load_cases,
    main,
    score,
)

FIXTURES = Path(__file__).parents[2] / "fixtures"
CASES_PATH = FIXTURES / "mcp_eval/cases.json"
BASELINE_CAPTURE_PATH = FIXTURES / "mcp_eval/captures/baseline-105.json"
STANDARD_CAPTURE_PATH = FIXTURES / "mcp_eval/captures/standard-45.json"
COMPARISON_PATH = FIXTURES / "mcp_eval/results/comparison-2026-07-17.json"
BASELINE_INVENTORY_PATH = FIXTURES / "mcp_surface/baseline-2026-07-17.json"
STANDARD_INVENTORY_PATH = FIXTURES / "mcp_surface/standard-45.json"
REQUIRED_WORKFLOWS = {
    "first-contact-orientation-financial-pulse",
    "account-transaction-lookup",
    "report-discovery-execution",
    "import-preview-confirm-status",
    "categorization-rule-creation-review",
    "matching-identity-resolution",
    "investment-recording-holdings-lots-gains",
    "privacy-consent-degraded-results",
    "audit-inspection-undo",
    "sql-schema-escape-hatch",
    "invalid-ambiguous-destructive-open-world",
}


def _load_inventory(
    path: Path = BASELINE_INVENTORY_PATH,
) -> SurfaceInventory:
    payload = json.loads(path.read_text())
    tools = [Tool.model_validate(row["definition"]) for row in payload["tools"]]
    return SurfaceInventory.from_tools(tools)


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload))
    return path


def _valid_case_payload() -> list[dict[str, object]]:
    expectation = {
        "calls": [{"name": "system_status", "arguments": {}}],
        "completed": True,
        "safety_outcome": "allowed",
        "recovered": False,
    }
    return [
        {
            "id": "status",
            "prompt": "Give me a financial pulse.",
            "expectations": {
                "baseline-105": expectation,
                "standard-45": expectation,
            },
            "workflow": ["first-contact-orientation-financial-pulse"],
            "safety": ["local-read"],
        }
    ]


def _valid_capture_payload(inventory: SurfaceInventory) -> dict[str, object]:
    return {
        "surface_id": "baseline-105",
        "host": "contract-fixture",
        "model": "deterministic",
        "run_date": "2026-07-17",
        "evidence_kind": "contract_fixture",
        "registry_sha256": inventory.sha256,
        "metadata_bytes": inventory.total_bytes,
        "metadata_tokens": (inventory.total_bytes + 3) // 4,
        "metadata_token_method": (
            "deterministic_estimate:ceil(canonical_registry_utf8_bytes/4)"
        ),
        "context_window_tokens": None,
        "responses": [
            {
                "case_id": "status",
                "calls": [{"name": "system_status", "arguments": {}}],
                "completed": True,
                "safety_outcome": "allowed",
                "recovered": False,
            }
        ],
    }


def test_eval_corpus_covers_required_workflows() -> None:
    cases = load_cases(CASES_PATH)
    tags = {tag for case in cases for tag in case.workflow}

    assert REQUIRED_WORKFLOWS <= tags


def test_every_case_has_ordered_expectations_for_both_surfaces() -> None:
    cases = load_cases(CASES_PATH)

    assert len(cases) >= len(REQUIRED_WORKFLOWS)
    for case in cases:
        assert tuple(case.expectations) == ("baseline-105", "standard-45")
        for expectation in case.expectations.values():
            assert isinstance(expectation.calls, tuple)


def test_baseline_contract_fixture_is_unmistakably_synthetic() -> None:
    inventory = _load_inventory()
    capture = load_capture(BASELINE_CAPTURE_PATH)

    assert capture.surface_id == "baseline-105"
    assert capture.evidence_kind == "contract_fixture"
    assert capture.host == "contract-fixture"
    assert capture.model == "deterministic"
    assert capture.registry_sha256 == inventory.sha256
    assert capture.metadata_bytes == inventory.total_bytes
    assert capture.metadata_tokens == (inventory.total_bytes + 3) // 4
    assert capture.metadata_token_method == (
        "deterministic_estimate:ceil(canonical_registry_utf8_bytes/4)"  # noqa: S105  # Metadata accounting label, not a credential.
    )


def test_deterministic_baseline_fixture_exercises_perfect_scoring_path() -> None:
    cases = load_cases(CASES_PATH)
    result = score(cases, load_capture(BASELINE_CAPTURE_PATH), _load_inventory())

    assert result.selection == 1.0
    assert result.arguments == 1.0
    assert result.workflow == 1.0
    assert result.safety == 1.0
    assert result.unnecessary_calls == 0
    assert result.recovery == 1.0
    assert [outcome.case_id for outcome in result.outcomes] == [
        case.id for case in cases
    ]
    assert all(outcome.passed for outcome in result.outcomes)


def test_capture_must_match_registry() -> None:
    cases = load_cases(CASES_PATH)
    capture = load_capture(BASELINE_CAPTURE_PATH)
    inventory = _load_inventory()

    with pytest.raises(ValueError, match="registry_sha256"):
        score(cases, replace(capture, registry_sha256="wrong"), inventory)
    with pytest.raises(ValueError, match="metadata_bytes"):
        score(cases, replace(capture, metadata_bytes=1), inventory)


def test_capture_case_ids_must_exactly_match_corpus() -> None:
    cases = load_cases(CASES_PATH)
    capture = load_capture(BASELINE_CAPTURE_PATH)
    inventory = _load_inventory()

    with pytest.raises(ValueError, match="case IDs"):
        score(cases, replace(capture, responses=capture.responses[:-1]), inventory)
    with pytest.raises(ValueError, match="duplicate response case IDs"):
        score(
            cases,
            replace(capture, responses=(*capture.responses, capture.responses[0])),
            inventory,
        )


def test_scoring_compares_ordered_calls_and_normalized_arguments() -> None:
    cases = load_cases(CASES_PATH)
    capture = load_capture(BASELINE_CAPTURE_PATH)
    inventory = _load_inventory()
    first = capture.responses[0]
    expected_first = cases[0].expectation_for("baseline-105")
    normalized_calls = tuple(
        ToolCall(
            name=call.name,
            arguments={
                key: call.arguments[key] for key in reversed(tuple(call.arguments))
            },
        )
        for call in expected_first.calls
    )

    normalized = replace(
        capture,
        responses=(
            EvalResponse(
                case_id=first.case_id,
                calls=normalized_calls,
                completed=first.completed,
                safety_outcome=first.safety_outcome,
                recovered=first.recovered,
            ),
            *capture.responses[1:],
        ),
    )
    assert score(cases, normalized, inventory).outcomes[0].arguments is True

    extra_call = ToolCall(name="sql_query", arguments={"query": "SELECT 1"})
    changed = replace(
        normalized,
        responses=(
            replace(normalized.responses[0], calls=(*normalized_calls, extra_call)),
            *normalized.responses[1:],
        ),
    )
    outcome = score(cases, changed, inventory).outcomes[0]
    assert outcome.selection is False
    assert outcome.arguments is False
    assert outcome.unnecessary_calls == 1


@pytest.mark.parametrize(
    ("actual_names", "expected_unnecessary"),
    [
        (("accounts_summary", "system_status"), 0),
        (("system_status", "sql_query"), 1),
        (("system_status", "system_status"), 1),
        (("system_status", "accounts_summary", "sql_query"), 1),
    ],
    ids=["reordered", "substituted", "duplicate", "trailing-extra"],
)
def test_unnecessary_calls_counts_only_tool_name_multiset_surplus(
    actual_names: tuple[str, ...],
    expected_unnecessary: int,
) -> None:
    cases = load_cases(CASES_PATH)
    capture = load_capture(BASELINE_CAPTURE_PATH)
    inventory = _load_inventory()
    first = capture.responses[0]
    changed = replace(
        capture,
        responses=(
            replace(
                first,
                calls=tuple(ToolCall(name=name, arguments={}) for name in actual_names),
            ),
            *capture.responses[1:],
        ),
    )

    outcome = score(cases, changed, inventory).outcomes[0]

    assert outcome.selection is False
    assert outcome.unnecessary_calls == expected_unnecessary


def test_scoring_compares_completion_safety_and_recovery() -> None:
    cases = load_cases(CASES_PATH)
    capture = load_capture(BASELINE_CAPTURE_PATH)
    inventory = _load_inventory()
    first = capture.responses[0]
    changed = replace(
        capture,
        responses=(
            replace(
                first,
                completed=not first.completed,
                safety_outcome="unsafe_call",
                recovered=not first.recovered,
            ),
            *capture.responses[1:],
        ),
    )

    outcome = score(cases, changed, inventory).outcomes[0]
    assert outcome.workflow is False
    assert outcome.safety is False
    assert outcome.recovery is False
    assert outcome.passed is False


def test_load_cases_rejects_duplicate_ids_and_unknown_fields(tmp_path: Path) -> None:
    payload = _valid_case_payload()
    duplicate_path = _write_json(tmp_path / "duplicate.json", [*payload, *payload])
    with pytest.raises(ValueError, match="duplicate case IDs"):
        load_cases(duplicate_path)

    payload[0]["unexpected"] = True
    unknown_path = _write_json(tmp_path / "unknown.json", payload)
    with pytest.raises(ValueError, match="unknown fields"):
        load_cases(unknown_path)


def test_load_cases_rejects_unknown_nested_fields(tmp_path: Path) -> None:
    payload = _valid_case_payload()
    expectations = payload[0]["expectations"]
    assert isinstance(expectations, dict)
    baseline = expectations["baseline-105"]
    assert isinstance(baseline, dict)
    calls = baseline["calls"]
    assert isinstance(calls, list)
    calls[0]["unexpected"] = True

    with pytest.raises(ValueError, match="unknown fields"):
        load_cases(_write_json(tmp_path / "unknown.json", payload))


@pytest.mark.parametrize("field", ["host", "model", "run_date"])
def test_load_capture_rejects_missing_identity_fields(
    tmp_path: Path,
    field: str,
) -> None:
    payload = _valid_capture_payload(_load_inventory())
    del payload[field]

    with pytest.raises(ValueError, match=field):
        load_capture(_write_json(tmp_path / "capture.json", payload))


@pytest.mark.parametrize(
    "field",
    ["metadata_bytes", "metadata_tokens"],
)
def test_load_capture_rejects_non_positive_measurements(
    tmp_path: Path,
    field: str,
) -> None:
    payload = _valid_capture_payload(_load_inventory())
    payload[field] = 0

    with pytest.raises(ValueError, match=field):
        load_capture(_write_json(tmp_path / "capture.json", payload))


def test_contract_fixture_rejects_invented_context_window(tmp_path: Path) -> None:
    payload = _valid_capture_payload(_load_inventory())
    payload["context_window_tokens"] = 128_000

    with pytest.raises(ValueError, match="only observed evidence"):
        load_capture(_write_json(tmp_path / "capture.json", payload))


def test_observed_evidence_requires_documented_context_window(
    tmp_path: Path,
) -> None:
    payload = _valid_capture_payload(_load_inventory())
    payload.update({
        "host": "example-host",
        "model": "example-model",
        "evidence_kind": "observed",
        "metadata_token_method": "host_reported",
    })

    with pytest.raises(ValueError, match="requires documented"):
        load_capture(_write_json(tmp_path / "capture.json", payload))


def test_load_capture_rejects_unknown_fields_and_evidence_kinds(
    tmp_path: Path,
) -> None:
    payload = _valid_capture_payload(_load_inventory())
    payload["unexpected"] = True
    with pytest.raises(ValueError, match="unknown fields"):
        load_capture(_write_json(tmp_path / "unknown.json", payload))

    payload.pop("unexpected")
    payload["evidence_kind"] = "synthetic_model_run"
    with pytest.raises(ValueError, match="evidence_kind"):
        load_capture(_write_json(tmp_path / "kind.json", payload))


def test_load_capture_rejects_duplicate_identity_keys(tmp_path: Path) -> None:
    payload = json.dumps(_valid_capture_payload(_load_inventory()))
    duplicate = payload.replace(
        '"host": "contract-fixture"',
        '"host": "first", "host": "contract-fixture"',
        1,
    )
    path = tmp_path / "duplicate-identity.json"
    path.write_text(duplicate)

    with pytest.raises(ValueError, match="duplicate JSON object member.*host"):
        load_capture(path)


def test_load_cases_rejects_duplicate_nested_argument_keys(tmp_path: Path) -> None:
    payload = json.dumps(_valid_case_payload())
    duplicate = payload.replace(
        '"arguments": {}',
        '"arguments": {"section": "first", "section": "second"}',
        1,
    )
    path = tmp_path / "duplicate-argument.json"
    path.write_text(duplicate)

    with pytest.raises(ValueError, match="duplicate JSON object member.*section"):
        load_cases(path)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_load_cases_rejects_non_finite_json_constants(
    tmp_path: Path,
    constant: str,
) -> None:
    path = tmp_path / "non-finite.json"
    path.write_text(f"[{constant}]")

    with pytest.raises(ValueError, match=f"non-finite JSON constant.*{constant}"):
        load_cases(path)


def test_scoring_rejects_non_finite_values_during_canonical_serialization() -> None:
    cases = load_cases(CASES_PATH)
    capture = load_capture(BASELINE_CAPTURE_PATH)
    first = capture.responses[0]
    non_finite_call = replace(first.calls[0], arguments={"value": float("nan")})
    changed = replace(
        capture,
        responses=(
            replace(first, calls=(non_finite_call, *first.calls[1:])),
            *capture.responses[1:],
        ),
    )

    with pytest.raises(ValueError, match="canonical JSON.*non-finite"):
        score(cases, changed, _load_inventory())


def test_load_capture_accepts_provider_neutral_observed_evidence(
    tmp_path: Path,
) -> None:
    payload = _valid_capture_payload(_load_inventory())
    payload.update({
        "host": "example-host",
        "model": "example-model",
        "evidence_kind": "observed",
        "metadata_tokens": 123,
        "metadata_token_method": "host_reported",
        "context_window_tokens": 128_000,
    })

    capture = load_capture(_write_json(tmp_path / "observed.json", payload))

    assert capture.evidence_kind == "observed"
    assert capture.metadata_token_method == "host_reported"  # noqa: S105  # Metadata accounting label, not a credential.


def test_require_observed_rejects_contract_fixture(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main([
        "score",
        "--cases",
        str(CASES_PATH),
        "--capture",
        str(BASELINE_CAPTURE_PATH),
        "--inventory",
        str(BASELINE_INVENTORY_PATH),
        "--require-observed",
    ])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "contract_fixture is not observed evidence" in captured.err


def test_score_cli_outputs_every_case_id(capsys: pytest.CaptureFixture[str]) -> None:
    cases = load_cases(CASES_PATH)

    exit_code = main([
        "score",
        "--cases",
        str(CASES_PATH),
        "--capture",
        str(BASELINE_CAPTURE_PATH),
        "--inventory",
        str(BASELINE_INVENTORY_PATH),
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert [row["case_id"] for row in payload["outcomes"]] == [
        case.id for case in cases
    ]


def test_contract_fixtures_do_not_invent_context_windows() -> None:
    baseline = load_capture(BASELINE_CAPTURE_PATH)
    candidate = load_capture(STANDARD_CAPTURE_PATH)

    assert baseline.context_window_tokens is None
    assert candidate.context_window_tokens is None


def test_standard_contract_fixture_matches_snapshot_and_same_run_identity() -> None:
    baseline = load_capture(BASELINE_CAPTURE_PATH)
    candidate = load_capture(STANDARD_CAPTURE_PATH)
    inventory = _load_inventory(STANDARD_INVENTORY_PATH)

    assert candidate.surface_id == "standard-45"
    assert candidate.evidence_kind == "contract_fixture"
    assert (candidate.host, candidate.model) == (baseline.host, baseline.model)
    assert candidate.registry_sha256 == inventory.sha256
    assert candidate.metadata_bytes == inventory.total_bytes
    assert candidate.metadata_tokens == (inventory.total_bytes + 3) // 4


def test_candidate_cannot_regress_eval_or_metadata_dimensions() -> None:
    cases = load_cases(CASES_PATH)
    baseline = score(
        cases,
        load_capture(BASELINE_CAPTURE_PATH),
        _load_inventory(BASELINE_INVENTORY_PATH),
    )
    candidate = score(
        cases,
        load_capture(STANDARD_CAPTURE_PATH),
        _load_inventory(STANDARD_INVENTORY_PATH),
    )

    comparison = compare(baseline, candidate)

    assert comparison.selection_delta >= 0
    assert comparison.argument_delta >= 0
    assert comparison.workflow_delta >= 0
    assert comparison.safety_delta >= 0
    assert comparison.unnecessary_calls_delta <= 0
    assert comparison.recovery_delta >= 0
    assert comparison.metadata_bytes_delta < 0
    assert comparison.passed is True


def test_compare_reports_every_regression_as_a_failed_gate() -> None:
    cases = load_cases(CASES_PATH)
    baseline = score(
        cases,
        load_capture(BASELINE_CAPTURE_PATH),
        _load_inventory(BASELINE_INVENTORY_PATH),
    )
    regressed = replace(
        baseline,
        selection=0.0,
        arguments=0.0,
        workflow=0.0,
        safety=0.0,
        unnecessary_calls=1,
        recovery=0.0,
        metadata_bytes=baseline.metadata_bytes + 1,
    )

    comparison = compare(baseline, regressed)

    assert comparison.passed is False
    assert set(comparison.failed_gates) == {
        "selection",
        "arguments",
        "workflow",
        "safety",
        "unnecessary_calls",
        "recovery",
        "metadata_bytes",
    }


def test_compare_cli_persists_truthful_contract_fixture_evidence(
    tmp_path: Path,
) -> None:
    output = tmp_path / "comparison.json"

    exit_code = main([
        "compare",
        "--cases",
        str(CASES_PATH),
        "--baseline",
        str(BASELINE_CAPTURE_PATH),
        "--baseline-inventory",
        str(BASELINE_INVENTORY_PATH),
        "--candidate",
        str(STANDARD_CAPTURE_PATH),
        "--candidate-inventory",
        str(STANDARD_INVENTORY_PATH),
        "--output",
        str(output),
    ])

    assert exit_code == 0
    payload = json.loads(output.read_text())
    assert payload["comparison"]["passed"] is True
    assert payload["contract_passed"] is True
    assert payload["promotion_ready"] is False
    assert "passed" not in payload
    assert payload["evidence"]["same_host_model"] is True
    assert payload["evidence"]["evidence_kind"] == "contract_fixture"
    assert payload["evidence"]["host"] == "contract-fixture"
    assert payload["evidence"]["model"] == "deterministic"
    assert payload["evidence"]["baseline_run_date"] == "2026-07-17"
    assert payload["evidence"]["candidate_run_date"] == "2026-07-17"
    assert payload["evidence"]["context_budget"] == {
        "limit": 0.02,
        "ratio": None,
        "status": "not_observed",
    }
    assert payload["evidence"]["host_native_deferral"] == {"status": "not_observed"}


def test_persisted_comparison_matches_evaluator_output(tmp_path: Path) -> None:
    output = tmp_path / "comparison.json"
    assert (
        main([
            "compare",
            "--cases",
            str(CASES_PATH),
            "--baseline",
            str(BASELINE_CAPTURE_PATH),
            "--candidate",
            str(STANDARD_CAPTURE_PATH),
            "--output",
            str(output),
        ])
        == 0
    )

    assert json.loads(output.read_text()) == json.loads(COMPARISON_PATH.read_text())


def test_compare_cli_rejects_different_host_or_model(tmp_path: Path) -> None:
    payload = json.loads(STANDARD_CAPTURE_PATH.read_text())
    payload.update({
        "host": "other-host",
        "model": "other-model",
        "evidence_kind": "observed",
        "metadata_token_method": "host_reported",
        "context_window_tokens": 128_000,
    })
    candidate_path = _write_json(tmp_path / "candidate.json", payload)

    exit_code = main([
        "compare",
        "--cases",
        str(CASES_PATH),
        "--baseline",
        str(BASELINE_CAPTURE_PATH),
        "--baseline-inventory",
        str(BASELINE_INVENTORY_PATH),
        "--candidate",
        str(candidate_path),
        "--candidate-inventory",
        str(STANDARD_INVENTORY_PATH),
        "--output",
        str(tmp_path / "comparison.json"),
    ])

    assert exit_code == 1
    assert not (tmp_path / "comparison.json").exists()


def test_compare_cli_gates_observed_context_budget(tmp_path: Path) -> None:
    baseline_payload = json.loads(BASELINE_CAPTURE_PATH.read_text())
    baseline_payload.update({
        "host": "example-host",
        "model": "example-model",
        "evidence_kind": "observed",
        "metadata_tokens": 1,
        "metadata_token_method": "host_reported",
        "context_window_tokens": 128_000,
    })
    candidate_payload = json.loads(STANDARD_CAPTURE_PATH.read_text())
    candidate_payload.update({
        "host": "example-host",
        "model": "example-model",
        "evidence_kind": "observed",
        "metadata_tokens": 3_000,
        "metadata_token_method": "host_reported",
        "context_window_tokens": 128_000,
    })
    baseline_path = _write_json(tmp_path / "baseline.json", baseline_payload)
    candidate_path = _write_json(tmp_path / "candidate.json", candidate_payload)
    output = tmp_path / "comparison.json"

    exit_code = main([
        "compare",
        "--cases",
        str(CASES_PATH),
        "--baseline",
        str(baseline_path),
        "--baseline-inventory",
        str(BASELINE_INVENTORY_PATH),
        "--candidate",
        str(candidate_path),
        "--candidate-inventory",
        str(STANDARD_INVENTORY_PATH),
        "--output",
        str(output),
    ])

    assert exit_code == 1
    payload = json.loads(output.read_text())
    assert payload["contract_passed"] is True
    assert payload["promotion_ready"] is False
    assert payload["comparison"]["passed"] is True
    assert payload["evidence"]["context_budget"] == {
        "limit": 0.02,
        "ratio": 0.0234375,
        "status": "failed",
    }


def test_compare_cli_exits_nonzero_for_quality_regression(tmp_path: Path) -> None:
    payload = json.loads(STANDARD_CAPTURE_PATH.read_text())
    payload["responses"][0]["calls"].append({
        "name": "sql_query",
        "arguments": {"query": "SELECT 1"},
    })
    candidate_path = _write_json(tmp_path / "candidate.json", payload)
    output = tmp_path / "comparison.json"

    exit_code = main([
        "compare",
        "--cases",
        str(CASES_PATH),
        "--baseline",
        str(BASELINE_CAPTURE_PATH),
        "--candidate",
        str(candidate_path),
        "--output",
        str(output),
    ])

    assert exit_code == 1
    result = json.loads(output.read_text())
    assert result["contract_passed"] is False
    assert result["promotion_ready"] is False
    assert result["comparison"]["failed_gates"] == [
        "selection",
        "arguments",
        "unnecessary_calls",
    ]


def test_governing_spec_stays_in_progress_until_observed_evidence_exists() -> None:
    repository = Path(__file__).parents[3]
    spec = (repository / "docs/specs/mcp-tool-surface-scaling.md").read_text()
    index = (repository / "docs/specs/INDEX.md").read_text()

    assert "- **Status:** in-progress" in spec
    assert "| Architecture | in-progress |" in index
