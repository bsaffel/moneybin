"""R9: render a report's query in both provenance forms.

``WidgetCard``'s contract states that every widget showing a number must pass
``sql``. All three tiers satisfy it from one place — this renderer — so the
brass SQL chip is fed identically whether the report came from a decorator or a
database row.

Two forms, because the provenance ladder's bottom rung opens the query in the
SQL console for direct editing, where a template with an unbound ``$month``
would fail:

- ``sql`` — the executed form, parameters rendered as typed literals.
  **Display only.** MoneyBin never executes this string; it exists so a user can
  paste it into the console, where it re-enters through
  ``validate_read_only_query`` and normal parameterization.
- ``sql_template`` — the stored form, placeholders intact, character for
  character as the runner or the row produced it.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Final

import sqlglot
from sqlglot import exp
from sqlglot.errors import TokenError
from sqlglot.tokenizer_core import TokenType

from moneybin.privacy.redaction import is_safe_to_publish_verbatim
from moneybin.reports._framework.contract import (
    Binding,
    ReportQuery,
    bound_class,
    bound_value,
)

_DIALECT: Final = "duckdb"


@dataclass(frozen=True, slots=True)
class SqlForms:
    """One query rendered both ways, plus what each form had to withhold."""

    sql: str | None
    sql_template: str
    withheld_parameters: tuple[str, ...]
    """Bindings classed above LOW, which keep their placeholder in ``sql``."""
    suppressed_by: tuple[str, ...]
    """Parameters with no value at all, which suppress ``sql`` entirely."""


@dataclass(frozen=True, slots=True)
class _Slot:
    """One placeholder's span in the query text and how it identifies itself."""

    start: int
    end: int
    """Inclusive, as sqlglot's tokenizer reports it."""
    name: str
    """Empty for a positional ``?``, which carries no name at all."""
    position: int
    """0-based ordinal among all placeholders, in source order."""

    @property
    def label(self) -> str:
        """How this slot is named in a withheld/suppressed report."""
        return self.name or f"?{self.position + 1}"


def render_sql_forms(query: ReportQuery, *, unbound: Collection[str] = ()) -> SqlForms:
    """Render ``query``'s executed and template forms.

    ``unbound`` names parameters the caller could not supply a value for. They
    keep their placeholder and suppress the executed form: a template is a
    truthful artifact, but a half-rendered "executed" query is not one, and
    handing a reader SQL that cannot run under the label of the SQL that ran is
    the failure this whole surface exists to prevent.

    A parameter whose class is not safe to publish verbatim keeps its
    placeholder too, and that is the load-bearing rule. Rendering is not
    execution, so it never passes through ``run_report``'s ``classify_columns``
    / ``redact_records`` — a report filtered by routing number would otherwise
    return that value verbatim here while the same value is masked in every row
    of the result it explains. The test is
    :func:`~moneybin.privacy.redaction.is_safe_to_publish_verbatim` rather than a
    tier comparison, because a FLOORED binding is ``Tier.LOW`` and still masks
    per value — a tier-only test rendered it in the clear.
    Masking the value instead (``'****1234'``) would be worse on both counts: it
    is not valid SQL for the column it filters, and it invites the reader to
    believe the string is the query that ran.
    """
    try:
        slots = _slots(query.sql)
    except TokenError:
        # A runner that built SQL sqlglot cannot even tokenize still has a
        # template to show. Only the rendered form is lost, and nothing withheld.
        return SqlForms(
            sql=None,
            sql_template=query.sql,
            withheld_parameters=(),
            suppressed_by=(),
        )

    named = isinstance(query.params, Mapping)
    withheld: list[str] = []
    suppressed: list[str] = []
    literals: list[tuple[_Slot, str]] = []
    for slot in slots:
        binding = _binding_for(query, slot=slot, named=named)
        if slot.name and slot.name in unbound:
            suppressed.append(slot.name)
        elif binding is None or not is_safe_to_publish_verbatim(bound_class(binding)):
            withheld.append(slot.label)
        else:
            literals.append((slot, exp.convert(bound_value(binding)).sql(_DIALECT)))

    return SqlForms(
        # A suppressed parameter leaves a placeholder no reader asked for, so
        # the form is withdrawn rather than published half-rendered.
        sql=None if suppressed else _spliced(query.sql, literals),
        sql_template=query.sql,
        withheld_parameters=tuple(withheld),
        suppressed_by=tuple(suppressed),
    )


def _slots(sql: str) -> tuple[_Slot, ...]:
    """Every placeholder in ``sql``, in the order the text presents them.

    Read from the tokenizer rather than from the parse tree, for two reasons that
    both bite:

    1. **Order.** DuckDB binds positional ``?`` in source order, and no tree walk
       reproduces it — ``find_all`` is breadth-first, and even depth-first follows
       sqlglot's arg order, which puts a ``Select``'s ``limit`` ahead of its
       ``where``. Rendering by either would print values in slots they were never
       bound to: a query that claims numbers it did not run with, which is the
       silent mis-binding R8 exists to prevent, reappearing in the renderer.
    2. **Import-order independence.** Importing SQLMesh rewrites sqlglot's parser
       so ``$name`` becomes ``Parameter(Var)`` instead of ``Placeholder`` for the
       rest of the process. Token *types and offsets are identical either way*, so
       this path cannot depend on whether SQLMesh happens to be loaded.

    The tokenizer also knows what is a string literal, so a ``?`` inside quoted
    text is not a placeholder — which a textual scan for ``?`` would get wrong.
    """
    tokens = sqlglot.tokenize(sql, read=_DIALECT)
    slots: list[_Slot] = []
    index = 0
    position = 0
    while index < len(tokens):
        token = tokens[index]
        following = tokens[index + 1] if index + 1 < len(tokens) else None
        if token.token_type is TokenType.PLACEHOLDER:
            slots.append(_Slot(token.start, token.end, "", position))
            position += 1
        elif token.token_type is TokenType.PARAMETER and following is not None:
            # `$name` tokenizes as PARAMETER('$') followed by the name; the slot
            # spans both so the whole reference is replaced.
            slots.append(_Slot(token.start, following.end, following.text, position))
            position += 1
            index += 1
        index += 1
    return tuple(slots)


def _spliced(sql: str, literals: list[tuple[_Slot, str]]) -> str:
    """Substitute each rendered literal into its own span, right to left.

    Right to left so an earlier slot's offsets stay valid after a later one is
    replaced. Splicing the author's own text — rather than regenerating the query
    from a mutated tree — also means the pasteable form keeps the formatting the
    reader already recognizes.
    """
    rendered = sql
    for slot, literal in sorted(literals, key=lambda pair: pair[0].start, reverse=True):
        rendered = rendered[: slot.start] + literal + rendered[slot.end + 1 :]
    return rendered


def _binding_for(
    query: ReportQuery, *, slot: _Slot, named: bool
) -> Binding | object | None:
    """The binding governing one placeholder, or ``None`` when there is none.

    ``None`` is not "no class" — it is a placeholder the runner never bound,
    which cannot be rendered under any class and is withheld.
    """
    params = query.params
    if named:
        assert isinstance(params, Mapping)  # noqa: S101  # narrowed by the caller
        return params.get(slot.name)
    assert not isinstance(params, Mapping)  # noqa: S101  # narrowed by the caller
    if slot.position < len(params):
        return params[slot.position]
    return None
