"""Batch-level error projection for an all-failed import.

The top-level `error.code` is what agents branch on, so it must describe the
whole batch. These tests pin the two properties that makes true: it is
order-independent, and it only claims a specific code when every failed file
agrees on one.
"""

from typing import Any

from moneybin import error_codes
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta
from moneybin.protocol.import_envelope import mark_total_failure
from moneybin.services.import_service import BatchImportResult, PerFileResult


def _failed(path: str, code: str | None, hint: str | None) -> PerFileResult:
    return PerFileResult(
        path=path,
        status="failed",
        source_type=None,
        error=f"{path} broke",
        error_code=code,
        hint=hint,
    )


def _batch(*rows: PerFileResult) -> BatchImportResult:
    return BatchImportResult(
        per_file=list(rows),
        transforms_applied=False,
        transforms_duration_seconds=None,
    )


def _envelope() -> ResponseEnvelope[Any]:
    return ResponseEnvelope(
        summary=SummaryMeta(
            total_count=0, returned_count=0, has_more=False, sensitivity="low"
        ),
        data={"files": []},
    )


def test_unanimous_code_and_hint_are_hoisted() -> None:
    """One shared cause does describe the whole batch, so it carries up."""
    batch = _batch(
        _failed("a.csv", error_codes.INFRA_PERMISSION_DENIED, "grant access"),
        _failed("b.csv", error_codes.INFRA_PERMISSION_DENIED, "grant access"),
    )
    result = mark_total_failure(_envelope(), batch)

    assert result.status == "error"
    assert result.error is not None
    assert result.error.code == error_codes.INFRA_PERMISSION_DENIED
    assert result.error.hint == "grant access"


def test_mixed_causes_fall_back_instead_of_hoisting_the_first() -> None:
    """Two different causes means no single code is true of the batch."""
    batch = _batch(
        _failed("a.csv", error_codes.INFRA_PERMISSION_DENIED, "grant access"),
        _failed("b.csv", error_codes.INFRA_FILE_NOT_FOUND, "check the path"),
    )
    result = mark_total_failure(_envelope(), batch)

    assert result.error is not None
    assert result.error.code == error_codes.IMPORT_PARSE_ERROR
    # A wrong-but-plausible hint is worse than none — it would advise a chmod
    # for a file that is simply absent.
    assert result.error.hint is None


def test_batch_code_does_not_depend_on_file_order() -> None:
    """The regression: swapping the paths must not change the batch verdict."""
    first = _failed("a.csv", error_codes.INFRA_PERMISSION_DENIED, "grant access")
    second = _failed("b.csv", error_codes.INFRA_FILE_NOT_FOUND, "check the path")

    forward = mark_total_failure(_envelope(), _batch(first, second))
    reversed_ = mark_total_failure(_envelope(), _batch(second, first))

    assert forward.error is not None
    assert reversed_.error is not None
    assert forward.error.code == reversed_.error.code
    assert forward.error.hint == reversed_.error.hint


def test_unclassified_failures_use_the_domain_fallback() -> None:
    """No file carried a code, so the batch claims only "could not process"."""
    batch = _batch(_failed("a.csv", None, None), _failed("b.csv", None, None))
    result = mark_total_failure(_envelope(), batch)

    assert result.error is not None
    assert result.error.code == error_codes.IMPORT_PARSE_ERROR


def test_lone_failure_hoists_its_own_message() -> None:
    """With one file there are no others to over-claim for.

    The count message exists so a batch with several distinct causes doesn't
    adopt the first file's reason. At one file that risk is absent, and
    "Import failed for all 1 file(s); see data.files[]" buries the single
    reason the caller needs behind a pointer to a list of one. This is the
    common shape now that a single-file CLI import reports as a one-file batch.
    """
    batch = _batch(_failed("a.csv", error_codes.INFRA_PERMISSION_DENIED, "grant"))
    result = mark_total_failure(_envelope(), batch)

    assert result.error is not None
    assert result.error.message == "a.csv broke"


def test_distinct_messages_fall_back_to_the_count() -> None:
    """Two different reasons: neither one is true of the batch."""
    batch = _batch(
        _failed("a.csv", error_codes.INFRA_PERMISSION_DENIED, "grant"),
        _failed("b.csv", error_codes.INFRA_PERMISSION_DENIED, "grant"),
    )
    result = mark_total_failure(_envelope(), batch)

    assert result.error is not None
    assert "all 2 file(s)" in result.error.message
    # The code still hoists — it IS unanimous here. Message and code are
    # decided independently, each on its own agreement.
    assert result.error.code == error_codes.INFRA_PERMISSION_DENIED


def test_unclassified_lone_failure_still_names_the_count() -> None:
    """A file that carried no message contributes none to hoist.

    Guards the `or` fallback: the unanimity set is `{None}` here, which is
    "unanimous" by length but carries nothing a caller could act on.
    """
    batch = _batch(
        PerFileResult(path="a.csv", status="failed", source_type=None, error=None)
    )
    result = mark_total_failure(_envelope(), batch)

    assert result.error is not None
    assert "all 1 file(s)" in result.error.message


def test_partial_success_stays_ok() -> None:
    """At least one import is genuine success — the batch must not flip."""
    batch = _batch(
        PerFileResult(path="a.csv", status="imported", source_type="tabular"),
        _failed("b.csv", error_codes.INFRA_FILE_NOT_FOUND, "check the path"),
    )
    result = mark_total_failure(_envelope(), batch)

    assert result.status == "ok"
    assert result.error is None


def test_all_failed_envelope_keeps_its_per_file_payload() -> None:
    """`data.files[]` is where the detail lives — the gate must not discard it."""
    envelope = ResponseEnvelope(
        summary=SummaryMeta(
            total_count=2, returned_count=2, has_more=False, sensitivity="low"
        ),
        data={"files": [{"path": "a.csv"}, {"path": "b.csv"}]},
    )
    result = mark_total_failure(
        envelope,
        _batch(_failed("a.csv", None, None), _failed("b.csv", None, None)),
    )

    assert result.data["files"] == [{"path": "a.csv"}, {"path": "b.csv"}]
    assert result.to_dict()["data"]["files"] != []
