"""Pipeline-execution harness primitives.

Distinct from ``moneybin.validation.assertions`` (which contain pure data
predicates): these helpers DRIVE pipeline operations (re-run, run with
empty input, run with bad input) and report on execution-time invariants
(no duplicate rows, no crash, expected error raised).

They live under ``tests/`` because they have no consumer outside the
scenario suite — ``data-reconciliation.md`` only consumes data
predicates. If a future runtime consumer emerges, lift the relevant
primitive into ``moneybin.validation``.
"""

from __future__ import annotations

from collections.abc import Callable

from moneybin.database import Database
from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_idempotent(
    db: Database,
    *,
    tables: list[str],
    rerun: Callable[[], None],
) -> AssertionResult:
    """Snapshot ``tables`` row counts, invoke ``rerun``, assert counts unchanged."""
    before = {t: _count(db, t) for t in tables}
    rerun()
    after = {t: _count(db, t) for t in tables}
    return AssertionResult(
        name="idempotent",
        passed=before == after,
        details={"before": before, "after": after},
    )


def assert_incremental_safe(
    db: Database,
    *,
    tables: list[str],
    load_a: Callable[[], None],
    load_b: Callable[[], None],
    expected_a_count: dict[str, int],
    expected_b_count: dict[str, int],
) -> AssertionResult:
    """Load A → assert counts; load B (overlapping) → assert only new rows added."""
    load_a()
    after_a = {t: _count(db, t) for t in tables}
    load_b()
    after_b = {t: _count(db, t) for t in tables}
    failures: list[str] = []
    for t in tables:
        if after_a[t] != expected_a_count.get(t):
            failures.append(
                f"{t} after-A: expected {expected_a_count.get(t)!r}, got {after_a[t]}"
            )
        if after_b[t] != expected_b_count.get(t):
            failures.append(
                f"{t} after-B: expected {expected_b_count.get(t)!r}, got {after_b[t]}"
            )
    return AssertionResult(
        name="incremental_safe",
        passed=not failures,
        details={
            "after_load_a": after_a,
            "after_load_b": after_b,
            "expected_a": expected_a_count,
            "expected_b": expected_b_count,
            "failures": failures,
        },
    )


def assert_empty_input_safe(
    db: Database,
    *,
    run: Callable[[], None],
    tables: list[str],
) -> AssertionResult:
    """Invoke ``run`` (with empty input pre-loaded); assert no crash and tables empty."""
    try:
        run()
    except Exception as exc:  # noqa: BLE001 — surface any failure as a result
        return AssertionResult(
            name="empty_input_safe",
            passed=False,
            details={"reason": "run raised", "exception_type": type(exc).__name__},
            error=str(exc),
        )
    counts = {t: _count(db, t) for t in tables}
    nonempty = {t: n for t, n in counts.items() if n > 0}
    return AssertionResult(
        name="empty_input_safe",
        passed=not nonempty,
        details={"row_counts": counts, "nonempty": nonempty},
    )


def assert_malformed_input_rejected(
    *,
    run: Callable[[], None],
    expected_message_substring: str,
    expected_exception_type: type[Exception] = Exception,
) -> AssertionResult:
    """Invoke ``run``; assert it raises the expected exception with a matching message.

    A wrong-type exception is reported as a failing AssertionResult rather
    than propagated, so callers always observe the harness contract.
    """
    try:
        run()
    except Exception as exc:  # noqa: BLE001 — surface any failure as a result
        if not isinstance(exc, expected_exception_type):
            return AssertionResult(
                name="malformed_input_rejected",
                passed=False,
                details={
                    "reason": "wrong exception type raised",
                    "expected_type": expected_exception_type.__name__,
                    "actual_type": type(exc).__name__,
                    "message_excerpt": str(exc)[:200],
                },
            )
        msg = str(exc)
        if expected_message_substring.lower() in msg.lower():
            return AssertionResult(
                name="malformed_input_rejected",
                passed=True,
                details={
                    "exception_type": type(exc).__name__,
                    "message_excerpt": msg[:200],
                },
            )
        return AssertionResult(
            name="malformed_input_rejected",
            passed=False,
            details={
                "reason": "exception raised but message did not match",
                "expected_substring": expected_message_substring,
                "actual_message": msg[:200],
            },
        )
    return AssertionResult(
        name="malformed_input_rejected",
        passed=False,
        details={"reason": "no exception was raised"},
    )


def assert_subprocess_parity(
    *,
    in_process_outputs: dict[str, int],
    subprocess_outputs: dict[str, int],
) -> AssertionResult:
    """Compare row counts from in-process vs subprocess runs; assert equal."""
    diff = {
        k: {
            "in_process": in_process_outputs.get(k),
            "subprocess": subprocess_outputs.get(k),
        }
        for k in set(in_process_outputs) | set(subprocess_outputs)
        if in_process_outputs.get(k) != subprocess_outputs.get(k)
    }
    return AssertionResult(
        name="subprocess_parity",
        passed=not diff,
        details={
            "in_process": in_process_outputs,
            "subprocess": subprocess_outputs,
            "diff": diff,
        },
    )


def _count(db: Database, table: str) -> int:
    row = db.execute(
        f"SELECT COUNT(*) FROM {quote_ident(table)}"  # noqa: S608  # validated identifier
    ).fetchone()
    return int(row[0]) if row else 0
