"""Regenerate the derived privacy-class module for runner-less reports.* views.

Usage:
    uv run python scripts/generate_derived_report_classes.py

Writes ``src/moneybin/reports/definitions/_derived_classes.py`` from
``derive_report_classes()`` (``privacy/report_class_derivation.py``),
excluding every view already covered by an ``@report`` runner's own
``classes=`` map. Run this whenever a runner-less ``reports.*`` model's
SELECT changes; CI fails via
``tests/privacy/test_sql_query.py::test_generated_classes_are_current`` if
the checked-in file goes stale.
"""

from __future__ import annotations

import sys
from pathlib import Path

import moneybin.tables as tables_module
from moneybin.privacy.report_class_derivation import derive_report_classes
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.registry import spec_of
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.tables import TableRef

_OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "moneybin"
    / "reports"
    / "definitions"
    / "_derived_classes.py"
)


def _tableref_name_by_key() -> dict[tuple[str, str], str]:
    """(schema, name) -> the TableRef constant's attribute name in moneybin.tables."""
    return {
        (ref.schema, ref.name): const_name
        for const_name, ref in vars(tables_module).items()
        if isinstance(ref, TableRef)
    }


def _runner_backed_keys() -> set[tuple[str, str]]:
    """(schema, view) for every view already covered by an @report runner."""
    return {(spec_of(r).view.schema, spec_of(r).view.name) for r in ALL_REPORTS}


def _render(generated: dict[tuple[str, str], dict[str, DataClass]]) -> str:
    """Render the generated module source as text.

    NOTE: `test_generated_classes_are_current` compares the imported
    `DERIVED_REPORT_CLASSES` dict value against a freshly derived one — not
    this function's rendered text against the checked-in file's bytes. A
    change here that alters only formatting (docstring wording, key
    ordering, whitespace) with no change to the underlying dict will NOT
    make that test fail, so the checked-in file can silently desync from
    what `_render` would now produce. Re-run `make generate-report-classes`
    after editing this function, even when the dict-shape is unchanged.
    """
    name_by_key = _tableref_name_by_key()
    missing = sorted(key for key in generated if key not in name_by_key)
    if missing:
        raise SystemExit(
            f"No TableRef constant in moneybin.tables for: {missing}. "
            "Add one before generating."
        )

    const_names = sorted({name_by_key[key] for key in generated})
    lines = [
        '"""Generated: privacy classes for reports.* views with no @report runner.',
        "",
        "DO NOT EDIT BY HAND. Regenerate with:",
        "    make generate-report-classes",
        "",
        "Replaces the former hand-written reports/definitions/_bridged_classes.py.",
        "Every entry here comes straight from derive_report_classes()",
        "(privacy/report_class_derivation.py), which parses the model's SQL and",
        "reuses resolve_output_classes — the same classifier that masks user SQL",
        "at runtime — so this file cannot drift from the model the way a",
        "hand-maintained bridge could.",
        "tests/privacy/test_sql_query.py::test_generated_classes_are_current",
        "fails CI if this file is stale.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from moneybin.privacy.taxonomy import DataClass",
        f"from moneybin.tables import {', '.join(const_names)}",
        "",
        "# (schema, view) -> {column: DataClass}. Excludes every view already",
        "# covered by an @report runner's own classes= map — see",
        "# generate_derived_report_classes.py.",
        "DERIVED_REPORT_CLASSES: dict[tuple[str, str], dict[str, DataClass]] = {",
    ]
    for key in sorted(generated):
        const_name = name_by_key[key]
        lines.append(f"    ({const_name}.schema, {const_name}.name): {{")
        for column, dc in generated[key].items():
            lines.append(f'        "{column}": DataClass.{dc.name},')
        lines.append("    },")
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    """Regenerate the checked-in module; return a process exit code."""
    runner_keys = _runner_backed_keys()
    derived = derive_report_classes()
    generated = {k: v for k, v in derived.items() if k not in runner_keys}
    if not generated:
        print(  # noqa: T201  # script output
            "No runner-less reports.* views remain; consider deleting "
            "_derived_classes.py and its import in reports_class_map()."
        )
    _OUTPUT_PATH.write_text(_render(generated))
    print(f"Wrote {len(generated)} view(s) to {_OUTPUT_PATH}")  # noqa: T201  # script output
    return 0


if __name__ == "__main__":
    sys.exit(main())
