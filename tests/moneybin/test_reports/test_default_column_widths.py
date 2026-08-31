"""Requirement 9: every in-tree report's default columns fit 80 characters.

80 is the width F1 was reproduced at, so a wider bar would let an
implementation satisfy `docs/specs/cli-output-coherence.md` while the reported
defect persists.

**What the measurement assumes.** A column's rendered width depends on the
user's data, which no test can know, so each column contributes a declared
representative value:

- A money column contributes ``−1,234,567.89`` — a seven-figure amount, chosen
  as the widest a personal ledger plausibly holds, so a report that fits here
  fits for any smaller one.
- A free-text column (a merchant name, an account display name, a description)
  contributes nothing but its header. Requirement 9 exempts free text from the
  no-elision rule precisely because its length is unbounded, so the guarantee
  it makes about those columns is that the *header* is never elided.
- An enum-valued column contributes the widest member of its own declared
  vocabulary, read off the constant the runner validates against. One sample
  for the whole `TXN_TYPE` class would distort: `balance_drift.status` holds
  `currency-mismatch` and `recurring.cadence` holds `monthly`, and measuring
  the second at the first's width costs a report a column for no reason.
- Every other class contributes a value of its real shape — a full ISO date, a
  minted content-hash id, an ISO currency code.

This is a structural guarantee, not a promise that no row ever wraps: it
catches a default set that is too wide by construction — nine columns, or an
unbounded source-provided id — which is the failure F1 reported.

Each report is measured against its **whole declared projection**, so every
column its default set names is present. That is the widest case: a parameter
combination that returns fewer columns can only render narrower.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from itertools import product

import pytest
from rich.console import Console
from rich.table import Table

import moneybin.reports.definitions as definitions
import moneybin.reports.service_reports as service_reports

# Ahead of `cli_register` deliberately: `moneybin.cli.__init__` imports the
# whole command tree, which reaches back into `cli_register`, so importing that
# module first hits a partially-initialised cycle. Importing anything under
# `moneybin.cli` first settles the package. MINUS is what the sample needs
# anyway — the glyph a money cell really carries.
from moneybin.cli.render import MINUS
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import RegisteredReport
from moneybin.reports._framework.cli_register import (
    resolve_default_columns,
    visible_columns,
)
from moneybin.reports._framework.contract import OutputColumn
from moneybin.reports._framework.registry import discover_reports, spec_of
from moneybin.reports.definitions._shared import (
    DRIFT_STATUSES,
    RECURRING_CADENCES,
    RECURRING_STATUSES,
    SPENDING_COMPARES,
)

MAX_WIDTH = 80

#: Columns whose values are free text, and whose width is therefore unbounded.
#: They contribute their header alone — see the module docstring.
_FREE_TEXT = frozenset({
    DataClass.USER_NOTE,
    DataClass.MERCHANT_NAME,
    DataClass.DESCRIPTION,
    DataClass.INSTITUTION,
})

#: One representative value per class, at the shape it really renders in.
_SAMPLE_BY_CLASS: Mapping[DataClass, str] = {
    DataClass.TXN_DATE: "2026-08-30",
    DataClass.TIMESTAMP_OBSERVABILITY: "2026-08-30T12:00:00",
    DataClass.CURRENCY: "USD",
    DataClass.CATEGORY: "Food & Drink",
    DataClass.AGGREGATE: "1,234",
    DataClass.TXN_TYPE: "currency-mismatch",
    # A minted content hash — `csv_` plus 16 hex, per `.claude/rules/
    # identifiers.md`. A source-provided `account_id` can be far longer, which
    # is the argument for keeping one out of a default set rather than for
    # widening this sample.
    DataClass.RECORD_ID: "csv_0123456789abcdef",
    DataClass.ACCOUNT_IDENTIFIER: "****1234",
}

_MONEY_SAMPLE = f"{MINUS}1,234,567.89"

#: (report, column) → the vocabulary a runner validates that column against.
#: Read off the constants rather than restated, so a widened vocabulary widens
#: the measurement with it. An enum column absent from this map contributes its
#: header alone; the guard below refuses that for any column a default set
#: actually renders.
_ENUM_VOCABULARIES: Mapping[tuple[str, str], Sequence[str]] = {
    ("core:balance_drift", "status"): DRIFT_STATUSES,
    ("core:recurring", "status"): RECURRING_STATUSES,
    ("core:recurring", "cadence"): RECURRING_CADENCES,
}

#: Parameter vocabularies that change a report's default column set. Only a
#: report declaring a *callable* default needs an entry; the guard below fails
#: if one appears without one, so this cannot go quietly stale.
_COLUMN_BEARING_PARAMETERS: Mapping[str, Mapping[str, Sequence[object]]] = {
    "core:spending": {"compare": SPENDING_COMPARES},
}


def _in_tree_reports() -> list[RegisteredReport]:
    """Every report defined in this repository, both kinds."""
    runner_backed = [spec_of(runner) for runner in discover_reports(definitions)]
    return [*runner_backed, *service_reports.SERVICE_REPORTS]


def _sample(report_id: str, column: OutputColumn) -> str:
    if column.money_kind is not None:
        return _MONEY_SAMPLE
    if column.data_class in _FREE_TEXT:
        return ""
    vocabulary = _ENUM_VOCABULARIES.get((report_id, column.name))
    if vocabulary is not None:
        return max(vocabulary, key=len)
    return _SAMPLE_BY_CLASS.get(column.data_class, "1,234")


def _rendered_width(report_id: str, columns: Sequence[OutputColumn]) -> int:
    """Characters the table needs with nothing wrapped or elided.

    Measured through Rich itself rather than by adding header lengths, so
    padding and border weights come from the renderer that will draw it. The
    console is given an effectively unbounded width because `measure` clamps to
    it otherwise, and 80 is the thing under test.
    """
    table = Table()
    for column in columns:
        table.add_column(column.name, overflow="fold")
    table.add_row(*[_sample(report_id, column) for column in columns])
    return Console(width=10_000).measure(table).maximum


def _parameter_combinations(report_id: str) -> list[dict[str, object]]:
    """Every legal parameter combination that can change the column set."""
    vocabularies = _COLUMN_BEARING_PARAMETERS.get(report_id)
    if not vocabularies:
        return [{}]
    names = list(vocabularies)
    return [
        dict(zip(names, values, strict=True))
        for values in product(*(vocabularies[name] for name in names))
    ]


@pytest.mark.parametrize("spec", _in_tree_reports(), ids=lambda spec: spec.report_id)
def test_every_report_declares_a_default_column_set(spec: RegisteredReport) -> None:
    """Requirement 6: an in-tree report never relies on the renderer's fit.

    Fitting to the terminal is what a surface does when nobody told it which
    columns matter — it keeps the ends and drops the middle by width alone.
    Every report here has an author who knows which columns answer its
    question, and that judgement is not recoverable from column widths: the
    fit would keep whichever end happened to be narrow.
    """
    assert spec.default_columns is not None, (
        f"{spec.report_id} has no default_columns, so a text reader gets "
        "whichever columns happen to fit rather than the ones that answer it"
    )


@pytest.mark.parametrize("spec", _in_tree_reports(), ids=lambda spec: spec.report_id)
def test_every_default_column_set_fits_eighty_characters(
    spec: RegisteredReport,
) -> None:
    """Requirement 9, over every parameter combination that changes the set."""
    by_name = {column.name: column for column in spec.columns}
    declared = [column.name for column in spec.columns]

    for parameters in _parameter_combinations(spec.report_id):
        # Requirement 6: a default set that does not resolve is a spec
        # violation. Checked against the raw declaration, not against
        # `visible_columns`, which intersects with the columns it is handed and
        # would make every surviving name trivially declared — a guard that
        # cannot fail. `validate_default_columns` takes callables on trust, so
        # a typo in one reaches a user's terminal as a quietly narrower table
        # and this is the only assertion standing between the two.
        names = resolve_default_columns(spec, parameters)
        assert set(names) <= set(declared), (
            f"{spec.report_id} with {parameters} names undeclared columns: "
            f"{', '.join(sorted(set(names) - set(declared)))}"
        )
        width = _rendered_width(spec.report_id, [by_name[name] for name in names])
        assert width <= MAX_WIDTH, (
            f"{spec.report_id} with {parameters} renders {width} characters "
            f"over {len(names)} columns: {', '.join(names)}"
        )


def test_every_parameter_aware_report_declares_its_vocabulary() -> None:
    """The hand-kept vocabulary map cannot go stale in either direction.

    A callable default set is only as tested as the parameter values fed to it,
    so a report acquiring one without an entry here would be measured at its
    defaults alone — and an entry left behind after a report went static would
    quietly test nothing.
    """
    parameter_aware = {
        spec.report_id for spec in _in_tree_reports() if callable(spec.default_columns)
    }

    assert parameter_aware == set(_COLUMN_BEARING_PARAMETERS)


def test_every_rendered_enum_column_declares_its_vocabulary() -> None:
    """An unmapped enum column would be measured at its header width.

    That silently understates it — `currency-mismatch` is eleven characters
    wider than the `status` header above it — so a default set could pass here
    and wrap in a real terminal. Only the columns a default set actually
    renders are required, because they are the only ones measured.
    """
    unmapped = {
        (spec.report_id, name)
        for spec in _in_tree_reports()
        for parameters in _parameter_combinations(spec.report_id)
        for name in visible_columns(
            spec,
            [column.name for column in spec.columns],
            parameters=parameters,
            wide=False,
        )
        if {column.name: column for column in spec.columns}[name].data_class
        is DataClass.TXN_TYPE
        and (spec.report_id, name) not in _ENUM_VOCABULARIES
    }

    assert not unmapped, f"enum columns measured at header width only: {unmapped}"
