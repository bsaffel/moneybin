"""No report-path ``UserError`` may interpolate author-chosen text into a message.

In text mode ``handle_cli_errors`` writes a ``UserError``'s ``message`` and
``hint`` through ``logger.error``, so both persist to the log file. A report
name, an output alias, and a parameter name are all author-chosen — ``amazon_spend``
is as plausible a merchant name as a filter one — and ``SanitizedLogFormatter``
has patterns for SSNs, long digit runs, and dollar amounts, none of which match an
arbitrary label. Such names belong in ``details``, which reaches the caller and the
JSON envelope and no durable log.

**Why this test is a source scan.** The same defect was found and fixed five times
across five review rounds, each time at whichever site a reviewer happened to walk
to next, because every fix was scoped to the file the last one was in. A
behavioural test only covers the refusals someone remembered to enumerate. This
enumerates them from the source instead, so the *next* interpolation fails here
rather than in review.

Its behavioural partner is
``test_dynamic.py::test_every_save_refusal_keeps_author_chosen_names_out_of_the_message``,
which drives the refusals with a merchant-shaped identifier. Neither replaces the
other: this one cannot tell whether an interpolation is dangerous, and that one
cannot see a refusal nobody thought to exercise.
"""

from __future__ import annotations

import ast
from pathlib import Path

import moneybin

#: Modules that can raise about a user-created report. Directories are swept
#: recursively, so a new module in one of them is covered without edit.
_REPORT_PATH = (
    "reports",
    "services/user_reports_service.py",
    "repositories/user_reports_repo.py",
    "cli/report_params.py",
    "cli/commands/reports",
    "exports/service.py",
    "privacy/report_materialization.py",
)

#: Interpolations reviewed and accepted, keyed by module-relative path. Set
#: equality against the live scan below, per the guard rule that a hand-kept list
#: is checked by equality and never by subset: an entry that stops appearing is as
#: much a signal as one that appears.
#:
#: Each value is the expression source as ``ast.unparse`` renders it. Everything
#: here is a repo constant, an enum value, a Python type name, or a fixed schema
#: field name — none is text an author chose.
_ACCEPTED: dict[str, set[str]] = {
    "reports/_framework/derive.py": {
        # The type token the author typed after the colon, in a closed grammar
        # position. Echoing it is what makes the error actionable — "Unsupported
        # parameter type 'flaot'" names the typo — and a type is not a label.
        "token",
        "getattr(annotation, '__name__', annotation)",
        "', '.join(sorted(_PARAM_TYPES))",
        "', '.join(sorted(SAVE_SCHEMAS))",
        "data_class.value",
        "data_class.tier.name",
    },
    "services/user_reports_service.py": {
        # The *field* name ("name", "description", "query_sql") and a constant —
        # never the over-long value, which that refusal reports only by length.
        "field",
        "limit",
        "IDENTIFIER_MAX_LEN",
        "to_class.value",
        "from_class.value",
        # A minted opaque id (`uuid4[:12]`), not author text.
        "report.report_id",
    },
    "exports/service.py": {"resolved.kind", "destination_kind"},
}


def _interpolated(node: ast.AST | None) -> set[str]:
    """Source of every ``{...}`` expression inside an f-string argument."""
    if node is None:
        return set()
    return {
        ast.unparse(value.value)
        for sub in ast.walk(node)
        if isinstance(sub, ast.JoinedStr)
        for value in sub.values
        if isinstance(value, ast.FormattedValue)
    }


def _scan() -> dict[str, set[str]]:
    root = Path(moneybin.__file__).parent
    found: dict[str, set[str]] = {}
    for entry in _REPORT_PATH:
        target = root / entry
        files = sorted(target.rglob("*.py")) if target.is_dir() else [target]
        for file in files:
            tree = ast.parse(file.read_text())
            for node in ast.walk(tree):
                if not (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "UserError"
                ):
                    continue
                message = node.args[0] if node.args else None
                hint = next(
                    (kw.value for kw in node.keywords if kw.arg == "hint"), None
                )
                exprs = _interpolated(message) | _interpolated(hint)
                if exprs:
                    key = str(file.relative_to(root))
                    found.setdefault(key, set()).update(exprs)
    return found


def test_no_report_user_error_interpolates_unreviewed_text() -> None:
    """Every interpolation in a report-path refusal is a reviewed, non-author value.

    A failure here is not automatically a leak — it means a new interpolation
    appeared that nobody classified. Resolve it one of two ways: if the value is a
    constant, enum, type, or fixed field name, add it to ``_ACCEPTED`` with the
    reason; if it is a report name, an output alias, a parameter name, or any other
    text the author chose, move it out of ``message``/``hint`` and into ``details``.
    """
    assert _scan() == _ACCEPTED


def test_the_scan_reaches_the_modules_it_claims_to_cover() -> None:
    """A silent zero would make the guard above vacuously green.

    The scan walks paths by name, so a rename or a move would leave it passing
    while covering nothing — the failure mode a source-scan guard is most prone
    to. Assert it still finds the module every one of these refusals lives in.
    """
    root = Path(moneybin.__file__).parent
    for entry in _REPORT_PATH:
        assert (root / entry).exists(), f"{entry} no longer exists; fix _REPORT_PATH"
    assert "reports/_framework/derive.py" in _scan()
