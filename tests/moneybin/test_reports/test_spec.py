"""Tests for ReportSpec and ParamSpec dataclasses."""

from moneybin.reports._framework.spec import ParamSpec, ReportSpec


def test_paramspec_records_required_metadata() -> None:
    p = ParamSpec(
        name="year",
        type_hint="int",
        optional=True,
        default=None,
        doc="Filter to specific year (null = all years)",
    )
    assert p.name == "year"
    assert p.type_hint == "int"
    assert p.optional is True
    assert p.default is None
    assert p.doc.startswith("Filter")


def test_reportspec_carries_full_parsed_metadata() -> None:
    spec = ReportSpec(
        name="seasonal_spending",
        description="Seasonal spending breakdown by category and year",
        params=[
            ParamSpec(
                name="year", type_hint="int", optional=True, default=None, doc=""
            ),
        ],
        examples=["reports_seasonal_spending(year=2025)"],
        source_path=None,  # not required for unit-construction
    )
    assert spec.name == "seasonal_spending"
    assert len(spec.params) == 1
    assert spec.examples == ["reports_seasonal_spending(year=2025)"]


def test_reportspec_param_lookup_by_name() -> None:
    spec = ReportSpec(
        name="x",
        description="",
        params=[
            ParamSpec(name="a", type_hint="int", optional=True, default=None, doc=""),
            ParamSpec(name="b", type_hint="str", optional=False, default=None, doc=""),
        ],
        examples=[],
        source_path=None,
    )
    assert spec.param("a").type_hint == "int"
    assert spec.param("b").optional is False
