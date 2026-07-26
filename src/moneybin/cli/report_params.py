"""One binder for the CLI's ``--param`` flags, shared by every report surface.

R5 of ``docs/specs/reports-dynamic.md`` requires the binder to coerce each value
to its ``ParamSpec.annotation`` *before* the report is invoked: ``--param top=5``
arrives as the string ``"5"``, and a runner declaring ``top: int`` would
otherwise receive it raw and fail somewhere inside its own body — an error about
the report's internals for what is a boundary mistake. That must happen in one
place, so ``export report`` and ``reports run`` bind identically rather than each
growing its own rules.

Two grammars, one per direction, and no command accepts both:

- **Binding** a value — ``name=value`` (``reports run``, ``export report``).
- **Declaring** a parameter — ``name[:type][=default]`` (``reports create/set``),
  which is what a user report stores instead of a runner signature.
"""

from __future__ import annotations

import json
import types
import typing
from typing import TYPE_CHECKING, Any, cast, get_args, get_origin

import typer
from pydantic import JsonValue, TypeAdapter, ValidationError

from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ParamSpec

if TYPE_CHECKING:
    from moneybin.reports._framework.catalog import ReportCatalog


def parse_report_parameters(
    catalog: ReportCatalog,
    report_id: str,
    raw_parameters: list[str] | None,
) -> dict[str, JsonValue]:
    """Bind repeated ``--param key=value`` options through a report's annotations."""
    from moneybin.reports._framework.catalog import ServiceReportSpec

    spec = catalog.resolve(report_id)
    declared = spec.parameters if isinstance(spec, ServiceReportSpec) else spec.params
    annotations = {parameter.name: parameter.annotation for parameter in declared}
    supplied: dict[str, JsonValue] = {}
    for raw in raw_parameters or []:
        name, separator, value = raw.partition("=")
        if separator != "=" or not name:
            raise typer.BadParameter(
                "report parameters must use key=value",
                param_hint="--param",
            )
        if name in supplied:
            raise typer.BadParameter(
                f"report parameter {name!r} was supplied more than once",
                param_hint="--param",
            )
        annotation = annotations.get(name, str)
        supplied[name] = parse_parameter_value(value, annotation)

    _, validated = catalog.resolve_request(
        report_id=report_id,
        parameters=supplied,
        limit=0,
    )
    return validated


def parse_parameter_value(raw: str, annotation: object) -> JsonValue:
    """Apply the report parameter's declared type to one CLI string value."""
    adapter = TypeAdapter(Any if annotation is None else annotation)
    try:
        if _annotation_accepts_container(annotation):
            parsed = json.loads(raw)
            return cast(JsonValue, adapter.validate_python(parsed))
        if raw == "null" and _annotation_accepts_none(annotation):
            return cast(JsonValue, adapter.validate_python(None))
        return cast(JsonValue, adapter.validate_strings(raw))
    except (json.JSONDecodeError, ValidationError) as exc:
        raise typer.BadParameter(
            f"report parameter value {raw!r} does not match {annotation}",
            param_hint="--param",
        ) from exc


def parse_parameter_declaration(raw: str) -> ParamSpec:
    """Read one ``name[:type][=default]`` declaration into a :class:`ParamSpec`.

    ``data_class`` is deliberately ``UNRESOLVED`` here and ignored downstream: a
    parameter's class is derived from the comparison it appears in, never
    declared, so a user cannot widen their own masking floor by asserting one.

    An omitted type means ``str`` — a query parameter is text until its author
    says otherwise — and a parameter is required exactly when it declares no
    default, which is the same rule the stored ``params`` JSON round-trips.
    """
    from moneybin.reports._framework.derive import annotation_of

    head, has_default, default = raw.partition("=")
    name, _, token = head.partition(":")
    if not name:
        raise typer.BadParameter(
            f"parameter declaration {raw!r} names no parameter; "
            "use name[:type][=default]",
            param_hint="--param",
        )
    annotation = annotation_of(token or "str")
    return ParamSpec(
        name=name,
        annotation=annotation,
        default=parse_parameter_value(default, annotation) if has_default else None,
        required=not has_default,
        help="",
        data_class=DataClass.UNRESOLVED,
    )


def _annotation_accepts_container(annotation: object) -> bool:
    """Return whether JSON container syntax is meaningful for a parameter."""
    origin = get_origin(annotation)
    if origin in (list, dict):
        return True
    if origin in (typing.Union, types.UnionType):
        return any(_annotation_accepts_container(arm) for arm in get_args(annotation))
    return False


def _annotation_accepts_none(annotation: object) -> bool:
    """Return whether the declared report parameter accepts JSON null."""
    origin = get_origin(annotation)
    return annotation is type(None) or (
        origin in (typing.Union, types.UnionType) and type(None) in get_args(annotation)
    )
