"""The runner-first report contract.

A report is a decorated runner ``(db, **params) -> ReportQuery``. The runner
owns parameter validation, free-text→id resolution, and SQL construction; the
framework introspects it into a :class:`ReportSpec` and generates the MCP tool,
CLI command, and ``TableRef`` wiring from that single definition. See
``docs/specs/extension-contracts.md`` §"Report contract".
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, get_args

from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import TableRef

# A runner takes an open Database plus keyword-only params and returns the
# parameterized SELECT to run. ``Any`` for the first arg avoids importing
# Database here purely for a type alias.
Runner = Callable[..., "ReportQuery"]
_REPORT_ID = re.compile(r"[a-z][a-z0-9_-]*:[a-z][a-z0-9_-]*")

#: Carries what a converted row started in, once the report's currency column
#: has been relabelled to the target. Requirement 10 wants the exact rate behind
#: any converted number, and `applied_rates` alone cannot give it: a report
#: holding two source currencies on one date publishes two rates, and the row
#: that used to say which one applied now says the target. Named for what it
#: holds rather than `source_*`, which in this codebase means the system a row
#: was imported from. Present only on a read that actually priced something — on
#: a segmented or already-in-target result it would duplicate the currency
#: column and imply a conversion the caller did not get.
#:
#: Beside `RecomputeDerived` rather than in `convert`, because an `on_converted`
#: callback is its main reader and report definitions are imported to build the
#: CLI: importing `convert` from one pulls `CurrencyService`, and polars behind
#: it, into every cold start (`test_cli_main_import_does_not_load_heavy_deps`).
ORIGINAL_CURRENCY_COLUMN = "original_currency_code"

#: ``report_id`` namespace owned by the user tier — the one tier whose reports
#: are database rows rather than code. ``mint_user_report_id`` produces it and
#: ``report_tier`` reads it; defined here beside the id grammar it belongs to.
USER_NAMESPACE = "user"

#: A saved report's ``name``. Both surfaces resolve a report by this string and
#: ``ReportSpec.cli_name`` derives a Typer command name from it, so it is held to
#: the same slug shape every shipped report's name already has.
USER_REPORT_NAME = re.compile(r"[a-z][a-z0-9_-]*")


@dataclass(frozen=True, slots=True)
class Binding:
    """One bound query value plus the class of the value **actually bound**.

    R9 of ``reports-dynamic.md``: the class attaches to the binding, not to the
    runner signature. ``balance_drift`` declares ``account: str`` — free text a
    user typed, which classifies as the account name it is — and binds
    ``AccountService.resolve_strict(account)``, a minted opaque ``account_id``
    (``RECORD_ID``, LOW). Neither class describes the other's value, so a
    renderer that recovered the class from the signature would print one value
    under another's class. Positional inference fails one step earlier still:
    runners append conditionally, so binding *N* is not a fixed offset into the
    signature.

    Report inspection reads the class off the binding it is about to render and
    never reconstructs it from anything else.
    """

    value: object
    data_class: DataClass


def bound_value(binding: object) -> object:
    """The value to hand DuckDB, tolerating an unclassed extension binding."""
    return binding.value if isinstance(binding, Binding) else binding


def bound_class(binding: object) -> DataClass:
    """The class governing ``binding``; an unclassed value fails closed.

    ``DataClass.UNRESOLVED`` is ``privacy.sql_lineage.FAIL_CLOSED_CLASS``, named
    directly here so this module stays clear of the sqlglot import chain. A
    runner outside this repo is not type-checked against :class:`Binding`, so
    the runtime answers for a bare value rather than crashing on it — and the
    answer is the one that withholds.
    """
    return binding.data_class if isinstance(binding, Binding) else DataClass.UNRESOLVED


@dataclass(frozen=True, slots=True)
class ReportQuery:
    """A parameterized read-only SELECT that a report runner returns.

    ``actions`` and ``period`` let the runner declare its own envelope
    enrichment — next-step hints and the human-readable window — since those
    are report-specific and the runner is the report's single definition.
    """

    sql: str
    params: Sequence[Binding] | Mapping[str, Binding] = ()
    """Positional ``?`` bindings, or a name→binding mapping for ``$name`` SQL.

    A dynamic report stores named placeholders (R8 of ``reports-dynamic.md``):
    positional storage would need a name→position map maintained beside the SQL,
    and adding a ``WHERE`` clause shifts every later position — mis-binding
    silently rather than raising. Built-in runners keep binding positionally.

    Typed as :class:`Binding` so pyright-strict makes every in-repo binding site
    declare its class — the completeness check R9 asks for, applied at the
    author's keyboard rather than in a test run. The runtime still accepts a
    bare value (see :func:`bound_class`) because an extension runner outside
    this repo is not checked against this contract."""
    actions: Sequence[str] = ()
    period: str | None = None


@dataclass(frozen=True, slots=True)
class ParamSpec:
    """One keyword-only runner parameter, introspected from the signature.

    Maps 1:1 to an MCP-tool argument, a Typer ``--flag``, and the runner
    keyword argument. ``annotation`` is the resolved Python type;
    ``required`` is true when the parameter has no default; ``data_class``
    controls redaction when the effective value is copied into result metadata.
    """

    name: str
    annotation: Any
    default: Any
    required: bool
    help: str
    data_class: DataClass


type MoneyKind = Literal["flow", "magnitude", "delta", "balance"]
"""What a money column's number means, so a renderer never has to guess.

Deliberately not named ``kind``: :class:`ReportSemantics` already has one, with
a different vocabulary and a different grain. That one is *report*-level, and
`spending` carries a ``magnitude`` and a ``delta`` in the same result, so one
report-level value cannot describe both columns. The two overlap in wording
(``flow``, and ``position`` ≈ ``balance``) without being the same thing; a
shared name would read as one concept and rot into two.

- ``flow`` — signed under the AGENTS.md accounting convention (negative =
  expense, positive = income).
- ``magnitude`` — a positive absolute quantity whose polarity is carried by the
  column rather than the value. `spending_trend.total_spend` is
  ``SUM(ABS(t.amount))``: an outflow that happens to be positive.
- ``delta`` — a signed *change in a magnitude*, where the sign means direction
  rather than income/expense. `spending_trend.mom_delta` is
  ``total_spend - prev_month_spend``, so positive means spending rose. Declares
  a :data:`Polarity`, because that is what decides whether a rise is good.
- ``balance`` — a position, not a movement.

Optional on :class:`OutputColumn`. There is no default kind: an undeclared
column is not money as far as the renderer is concerned, and reaches the table
through ``str()``. Defaulting to a kind would mean guessing which columns hold
amounts, which is the inference requirement 12 exists to remove — and the only
available signal, :class:`~moneybin.privacy.taxonomy.DataClass`, answers a
privacy question, not a rendering one. An extension keeps working undeclared,
rendering exactly as it did before the field existed, and opts in per column.
"""

type Polarity = Literal["expense", "income"]
"""Which direction is the favourable one for the quantity a ``delta`` measures.

A rise in spending and a rise in income are both ``+``; only the declaration
says which of them is good news.
"""

# Read off the aliases above rather than restated, so the runtime gate in
# `OutputColumn.__post_init__` cannot drift from the type a contributor sees.
_MONEY_KINDS: tuple[str, ...] = get_args(MoneyKind.__value__)
_POLARITIES: tuple[str, ...] = get_args(Polarity.__value__)


@dataclass(frozen=True, slots=True)
class OutputColumn:
    """One named report output with its meaning and privacy class."""

    name: str
    description: str
    data_class: DataClass
    money_kind: MoneyKind | None = None
    """How to render this column's amounts; ``None`` means it is not money."""
    polarity: Polarity | None = None
    """Required when ``money_kind`` is ``"delta"``; refused on the other kinds."""

    def __post_init__(self) -> None:
        """Reject an unrenderable declaration where it is written, not where it is read.

        ``Money`` refuses the delta-without-polarity pair too, but that check
        runs in the text renderer — ``money_columns(spec)``, which the generated
        CLI command reaches only *after* ``catalog.execute(...)`` has run the
        report and written its side effects — and a JSON or MCP caller never
        reaches it at all. Enforcing it at construction makes the broken
        contract unbuildable on every surface at once, which is what an
        out-of-repo author reading ``docs/specs/extension-contracts.md`` needs
        it to do.

        The vocabulary is checked here for that reason and one more: a
        ``Literal`` binds a type checker, and the author this contract is
        written for may not run one. Neither wrong value fails on its own —
        an unrecognized ``money_kind`` falls through the renderer to unsigned
        and uncoloured, and ``Money.style_for`` reads every polarity that is
        not ``"income"`` as expense, so ``polarity="up"`` inverts a delta's
        colours instead of raising. A polarity on a non-delta is refused rather
        than ignored because no kind but ``delta`` reads one, and silently
        dropping it tells an author their column is polarized when the rendered
        output will not be. ``Money`` is left alone: it is not part of the
        extension surface, and every in-repo construction of one is a literal
        pyright already checks.
        """
        if self.money_kind is not None and self.money_kind not in _MONEY_KINDS:
            raise ValueError(
                f"money column {self.name!r} declares an unknown money_kind "
                f"{self.money_kind!r}; expected one of {', '.join(_MONEY_KINDS)}"
            )
        if self.polarity is not None and self.polarity not in _POLARITIES:
            raise ValueError(
                f"money column {self.name!r} declares an unknown polarity "
                f"{self.polarity!r}; expected one of {', '.join(_POLARITIES)}"
            )
        if self.money_kind == "delta" and self.polarity is None:
            raise ValueError(
                f"money column {self.name!r} is a delta and must declare its polarity"
            )
        if self.money_kind != "delta" and self.polarity is not None:
            raise ValueError(
                f"money column {self.name!r} is not a delta, so its polarity "
                f"{self.polarity!r} would be ignored rather than applied"
            )


@dataclass(frozen=True, slots=True)
class ReportSemantics:
    """Financial interpretation metadata for a report's metrics.

    ``kind`` admits ``"unknown"`` and ``unit`` / ``sign`` / ``time_basis`` admit
    ``None`` so a user-created report can state that its financial
    interpretation is unknown. MoneyBin cannot derive these from an arbitrary
    ``SELECT``, and defaulting them would publish a claim about the user's query
    that nobody made — an agent reading ``sign: "natural"`` on a report whose
    author flipped the sign gets a confidently wrong answer, which is worse than
    getting none. Decorated reports still supply all three.
    """

    unit: str | None
    currency: str | None
    sign: str | None
    kind: Literal["position", "flow", "ratio", "count", "unknown"]
    valuation_basis: str | None
    fx_basis: str | None
    time_basis: str | None
    denominator: str | None
    comparison_window: str | None
    exclusions: tuple[str, ...]
    provenance: tuple[str, ...]
    fx_date: str | None = None
    """The column whose value dates each row for display conversion.

    Defaults to ``None`` — no declared date — which segments rather than
    converting. That is the safe direction: a report that has not said which
    column dates its rows cannot be converted at a defensible rate, and picking
    one by guessing (the only ``TXN_DATE`` column, say) would silently price a
    lifetime total at whichever date happened to be named first.
    """


type RecomputeDerived = Callable[[list[dict[str, Any]], str], None]
"""Repair a report's derived columns in place after its money columns converted.

Receives the converted rows and the currency they are now in. Mutates rather
than returns because it repairs a subset of columns on rows the caller already
owns; rebuilding them would invite a partial copy that silently drops one.
"""


@dataclass(frozen=True, slots=True)
class ReportSpec:
    """Introspected metadata for one report, built from its runner.

    Carries everything the dynamic registrars need to build the MCP tool, CLI
    command, and ``TableRef`` wiring without re-reading the source.

    ``classes`` is the report's **declared** column→DataClass map — the privacy
    contract. It is declared, not derived: SQLMesh deploys each report view as a
    ``SELECT * FROM <internal physical table>`` pointer, so lineage on the view
    body can't classify it (see ADR-013). ``redaction`` masks output columns by
    this map; any column absent from it fails closed (see ``classify``).
    """

    report_id: str
    name: str
    description: str
    view: TableRef | None
    """The ``reports.*`` view backing this report, or ``None`` when it is not
    graph-backed. A user-created report is evaluated at query time and has no
    SQLMesh view, so it cannot be ``kind FULL`` or join scheduled refresh."""
    runner: Runner
    classes: Mapping[str, DataClass]
    columns: tuple[OutputColumn, ...]
    semantics: ReportSemantics
    params: tuple[ParamSpec, ...] = ()
    examples: tuple[str, ...] = ()
    domain: str | None = None
    class_downgrades: Mapping[str, str] = field(default_factory=dict)
    """Column → why it is declared below its derived class. CI requires a reason
    for every downgrade; derivation over-classifies computed columns, and
    over-masking a BI surface is its own failure mode."""
    on_converted: RecomputeDerived | None = None
    """Repair for columns derived from this report's money columns, or ``None``.

    Only a report holding a value *computed from* an amount needs one — a label
    bucketed against thresholds, or a total that must keep equalling its parts.
    Display conversion prices each money column on its own, so without this the
    derived value keeps describing the original currency."""

    def __post_init__(self) -> None:
        if _REPORT_ID.fullmatch(self.report_id) is None:
            raise ValueError("report_id must use namespace:name")
        declared = {column.name: column.data_class for column in self.columns}
        if len(declared) != len(self.columns) or declared != dict(self.classes):
            raise ValueError(
                "columns and classes must declare the same output fields "
                "with identical privacy classes"
            )
        object.__setattr__(self, "classes", MappingProxyType(dict(self.classes)))

    @property
    def cli_name(self) -> str:
        """Typer command name, e.g. ``large-transactions``."""
        return self.name.replace("_", "-")

    @property
    def parameters(self) -> tuple[ParamSpec, ...]:
        """Unified catalog name for declared input parameters."""
        return self.params


def report(
    *,
    report_id: str,
    name: str,
    view: TableRef,
    classes: Mapping[str, DataClass],
    parameter_classes: Mapping[str, DataClass],
    columns: tuple[OutputColumn, ...],
    semantics: ReportSemantics,
    domain: str | None = None,
    class_downgrades: Mapping[str, str] | None = None,
    on_converted: RecomputeDerived | None = None,
) -> Callable[[Runner], Runner]:
    """Mark a runner as a report and attach its introspected :class:`ReportSpec`.

    The spec is stored on the function as ``_report_spec`` for later discovery;
    the runner itself is returned unchanged so it stays directly callable.

    Args:
        report_id: Stable namespaced report identifier.
        name: Canonical report name (underscore form). The MCP tool is
            ``reports_<name>`` and the CLI command is ``<name>`` with
            underscores rendered as hyphens.
        view: The ``reports.*`` ``TableRef`` the runner reads.
        classes: The declared output-column→DataClass map — the report's
            privacy contract. Must cover every column the view exposes; an
            undeclared column fails closed at redaction time. Declared (not
            lineage-derived) because the deployed SQLMesh view is a
            ``SELECT *`` pointer lineage can't classify (ADR-013).
        parameter_classes: Exact runner-parameter→DataClass map used to redact
            effective parameters before they enter result metadata.
        columns: Ordered output column descriptions and privacy classes.
        semantics: Financial interpretation metadata for the report metrics.
        domain: Optional MCP namespace tag.
        class_downgrades: Column → reason, for every column whose declared
            class sits below its CI-derived floor (``derive_report_classes``).
            Over-declaring never needs a reason; only a genuine downgrade does.
        on_converted: Repair for columns derived from this report's money
            columns, run after display conversion prices them. Only a report
            holding a value computed from an amount needs one.
    """
    # Imported lazily to avoid a contract<->introspect import cycle.
    from moneybin.reports._framework.introspect import build_spec

    def decorate(fn: Runner) -> Runner:
        fn._report_spec = build_spec(  # type: ignore[attr-defined]
            fn,
            report_id=report_id,
            name=name,
            view=view,
            classes=classes,
            parameter_classes=parameter_classes,
            columns=columns,
            semantics=semantics,
            domain=domain,
            class_downgrades=class_downgrades,
            on_converted=on_converted,
        )
        return fn

    return decorate
