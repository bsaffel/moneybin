"""`moneybin stats` rendering — requirements 23-25 of cli-output-coherence.md.

The database is stubbed: these tests are about what the command prints for a
given set of `app.metrics` rows, not about how the rows get there.
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.commands.stats import stats_command

runner = CliRunner()

_RECORDED = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _app() -> typer.Typer:
    """Wrap the command alone, so a test never builds the whole CLI."""
    app = typer.Typer()
    app.command(name="stats")(stats_command)
    return app


def _patch_rows(
    monkeypatch: pytest.MonkeyPatch, rows: Sequence[tuple[Any, ...]]
) -> None:
    """Stub the database so `stats` reads exactly ``rows``."""
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = list(rows)

    @contextmanager
    def fake_get_database(*_a: Any, **_kw: Any) -> Generator[MagicMock, None, None]:
        yield db

    monkeypatch.setattr("moneybin.cli.commands.stats.get_database", fake_get_database)


def test_two_label_sets_of_one_metric_render_as_distinguishable_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 23: no two rendered lines share a label.

    `app.metrics` partitions on (metric_name, metric_type, labels), so one
    metric with two label sets is two rows. Rendering the name alone prints
    the same words twice with different numbers beside them, which reads as a
    display bug rather than as two dimensions of one measurement.
    """
    _patch_rows(
        monkeypatch,
        [
            (
                "moneybin_import_duration_seconds",
                "histogram",
                '{"source_type": "plaid"}',
                4.0,
                3,
                _RECORDED,
            ),
            (
                "moneybin_import_duration_seconds",
                "histogram",
                '{"source_type": "csv"}',
                9.0,
                3,
                _RECORDED,
            ),
        ],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    assert "plaid" in result.output, result.output
    assert "csv" in result.output, result.output
    # Requirement 16: the dimension reaches the reader as prose, not as the
    # `key=value` fragment requirement 1 calls a table spelled badly. The label
    # name is renderer copy and gets spelled out; only the value is data.
    assert "source_type=" not in result.output, result.output
    assert "source type: plaid" in result.output, result.output


def test_a_non_duration_histogram_does_not_render_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 24: a metric that is not a duration does not render `s`.

    `moneybin_import_batch_size` observes *files*. The unit is declared, not
    derived: the name ends in a dimension (`_batch_size`), and the fact that
    the dimension is counted in files lives only in the declaration.
    """
    _patch_rows(
        monkeypatch,
        [
            (
                "moneybin_import_batch_size",
                "histogram",
                "{}",
                12.0,
                2,
                _RECORDED,
            )
        ],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    assert "files" in result.output, result.output
    # The half that a bare "declare a unit" change would pass while still
    # printing the old hardcoded suffix beside it.
    assert "12.00s" not in result.output, result.output


def test_a_duration_histogram_still_renders_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 24's other half — declaring a unit must not lose `s`."""
    _patch_rows(
        monkeypatch,
        [
            (
                "moneybin_export_duration_seconds",
                "histogram",
                "{}",
                1.5,
                1,
                _RECORDED,
            )
        ],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    assert "1.50 s" in result.output, result.output


def test_metrics_are_grouped_under_a_domain_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 25: group by domain with a header per group.

    One alphabetical list interleaves unrelated subsystems, so a reader
    scanning for import health reads past export and MCP rows to find it.
    """
    _patch_rows(
        monkeypatch,
        [
            ("moneybin_export_runs", "counter", "{}", 7.0, 1, _RECORDED),
            ("moneybin_import_records", "counter", "{}", 40.0, 1, _RECORDED),
        ],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    # A header is a line of its own. Substring-matching "Export" would pass on
    # the ungrouped list too, because the metric line already starts with the
    # word — the assertion has to be that the word appears *alone*.
    headers = [line.strip() for line in result.output.splitlines()]
    assert "Export delivery" in headers, result.output
    assert "Import pipeline" in headers, result.output
    # The registry declares import before export, and the blocks follow it —
    # alphabetical order by name would have reversed them.
    assert headers.index("Import pipeline") < headers.index("Export delivery")


def test_a_domain_header_and_its_metric_lines_agree_on_casing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An acronym is spelled once, so the two places it renders cannot drift.

    The header comes from the domain and the line from the metric name. Cased
    separately, `MCP` heads a block whose only row says `Mcp` — the same word
    twice, two ways, on adjacent lines.
    """
    _patch_rows(
        monkeypatch,
        [
            (
                "moneybin_mcp_tool_duration_seconds",
                "histogram",
                "{}",
                1.9,
                3,
                _RECORDED,
            )
        ],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    assert "Mcp" not in result.output, result.output
    assert result.output.count("MCP") == 2, result.output


def test_one_subsystem_declared_under_several_name_prefixes_gets_one_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 25 groups by subsystem, and a name prefix is not one.

    The Categorization section declares metrics under five literal prefixes —
    `categorization_`, `categorize_`, `auto_rule_`, `rule_`, and
    `merchant_exemplar_count`. Splitting on the first underscore scattered one
    subsystem across four headers that alphabetical order then separated, and
    filed the exemplar gauge under the unrelated Merchant block.
    """
    _patch_rows(
        monkeypatch,
        [
            ("moneybin_auto_rule_broad_pending", "gauge", "{}", 2.0, 1, _RECORDED),
            ("moneybin_categorization_auto_rate", "gauge", "{}", 0.8, 1, _RECORDED),
            ("moneybin_categorize_items", "counter", "{}", 9.0, 1, _RECORDED),
            ("moneybin_merchant_exemplar_count", "gauge", "{}", 5.0, 1, _RECORDED),
            ("moneybin_rule_conflicts_pending", "gauge", "{}", 1.0, 1, _RECORDED),
        ],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    headers = [line.strip() for line in result.output.splitlines()]
    assert headers.count("Categorization") == 1, result.output
    # The four the first-underscore split used to produce, plus the block the
    # exemplar gauge used to be filed under.
    for fragment in ("Auto", "Categorize", "Rule", "Merchant"):
        assert fragment not in headers, f"{fragment!r} still heads a block"


def test_a_metric_heads_under_its_own_subsystem_not_a_name_alike(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two subsystems sharing a leading word stay apart.

    `merchant_exemplar_count` is a categorization gauge and
    `merchant_link_confidence` is identity resolution. Keyed on the shared
    `merchant` token they read as one subsystem's health.
    """
    _patch_rows(
        monkeypatch,
        [
            ("moneybin_merchant_exemplar_count", "gauge", "{}", 5.0, 1, _RECORDED),
            (
                "moneybin_merchant_link_confidence",
                "histogram",
                "{}",
                0.9,
                2,
                _RECORDED,
            ),
        ],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    headers = [line.strip() for line in result.output.splitlines()]
    assert "Categorization" in headers, result.output
    assert "Merchant identity resolution" in headers, result.output


def test_an_undeclared_metric_still_prints_under_its_own_heading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A name `app.metrics` kept from an older version has no declared domain.

    It keeps its value rather than disappearing or raising, the way an
    undeclared histogram keeps its sum.
    """
    _patch_rows(
        monkeypatch,
        [("moneybin_since_renamed", "counter", "{}", 3.0, 1, _RECORDED)],
    )

    result = runner.invoke(_app(), [])

    assert result.exit_code == 0, result.output
    headers = [line.strip() for line in result.output.splitlines()]
    assert "Other" in headers, result.output
    assert "3" in result.output, result.output


def _registered_metrics() -> set[str]:
    """Every MoneyBin metric name as ``app.metrics`` stores it.

    The registry rather than this module's declarations, for the reason
    `_registered_histograms` gives: a metric declared beside its service still
    renders through `stats` and still needs a domain.

    ``metric.name`` unchanged, because that is the name `flush_to_duckdb`
    writes for every type — it strips the `_total` a counter declaration spells
    and the `_sum` a histogram sample carries, and both leave `metric.name`.
    An earlier version of this helper appended `_total` back for counters on
    the belief that the declaration's spelling is what gets persisted, which
    made both coverage tests compare the declarations against themselves and
    pass while every counter rendered under `Other`.
    `test_persistence.py::test_a_counter_declared_with_total_persists_without_it`
    is what pins the premise; this helper only relies on it.
    """
    from prometheus_client import REGISTRY

    return {
        metric.name
        for metric in REGISTRY.collect()
        if metric.name.startswith("moneybin_")
    }


def test_every_metric_declares_a_domain() -> None:
    """Requirement 25's coverage guard, in the direction that matters most.

    A metric added without a domain would print under `Other` — a silent
    demotion no reviewer sees in the diff, since the declaration that omits it
    looks complete on its own.
    """
    from moneybin.metrics.registry import METRIC_DOMAINS

    registered = _registered_metrics()
    assert registered, "no MoneyBin metrics registered — the scan proves nothing"
    assert registered <= set(METRIC_DOMAINS), (
        f"metrics with no declared domain: {sorted(registered - set(METRIC_DOMAINS))}"
    )


def test_no_domain_is_declared_for_a_metric_that_does_not_exist() -> None:
    """The other direction: a renamed metric leaves its domain entry behind.

    A stale entry is invisible — it matches nothing, so nothing misprints —
    which is why it has to fail here rather than wait to mislead the next
    reader of the table.
    """
    from moneybin.metrics.registry import METRIC_DOMAINS

    registered = _registered_metrics()
    assert registered, "no MoneyBin metrics registered — the scan proves nothing"
    assert set(METRIC_DOMAINS) <= registered, (
        "domains declared for metrics that no longer exist: "
        f"{sorted(set(METRIC_DOMAINS) - registered)}"
    )


def _registered_histograms() -> set[str]:
    """Every histogram the process has registered, read off the live registry.

    The registry rather than the module's own attributes: a histogram declared
    anywhere still renders through `stats`, so scanning one file would let a
    metric declared beside its service escape the unit contract entirely.

    Both callers import ``HISTOGRAM_UNITS`` before calling this, and that
    import is what binds `metrics.registry`'s declarations into the process
    registry — hence the assertion below that the scan found any at all.
    """
    from prometheus_client import REGISTRY

    return {
        metric.name
        for metric in REGISTRY.collect()
        if metric.type == "histogram" and metric.name.startswith("moneybin_")
    }


def test_every_histogram_declares_a_unit() -> None:
    """Requirement 24's coverage guard.

    The unit lives in a table beside the declarations rather than on them, so
    nothing in the type system fails when a new histogram is added without
    one. This is what fails instead — otherwise the omission surfaces as a
    `stats` line with no unit, months later, in front of a user.
    """
    from moneybin.metrics.registry import HISTOGRAM_UNITS

    registered = _registered_histograms()

    assert registered, "no histograms found — the scan is broken, not the registry"
    assert registered <= set(HISTOGRAM_UNITS), (
        f"histograms with no declared unit: {sorted(registered - set(HISTOGRAM_UNITS))}"
    )


def test_no_unit_is_declared_for_a_metric_that_does_not_exist() -> None:
    """The table's other direction: a stale entry outlives its metric.

    A rename leaves the old key behind, where it reads as coverage and hides
    that the renamed metric has no unit. The membership test above passes
    either way, because a superset satisfies it.
    """
    from moneybin.metrics.registry import HISTOGRAM_UNITS

    registered = _registered_histograms()

    assert registered, "no histograms found — the scan is broken, not the registry"
    assert set(HISTOGRAM_UNITS) <= registered, (
        f"units declared for metrics that no longer exist: "
        f"{sorted(set(HISTOGRAM_UNITS) - registered)}"
    )
