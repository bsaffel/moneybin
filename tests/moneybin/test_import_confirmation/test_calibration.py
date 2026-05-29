"""Calibration harness for the cross-channel confidence contract.

Loads the YAML corpus, runs each fixture through its channel's detector,
computes per-tier field-exact precision, and asserts the `high` band
clears the precision bar (>= 0.99 field-exact). While the bar is unmet,
`self_accept_high` MUST stay False by default — that gate is locked here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from moneybin.config import ImportSettings

_PRECISION_BAR = 0.99
_CORPUS_PATH = Path(__file__).parent / "fixtures" / "corpus.yaml"
# fixture_path values in corpus.yaml are relative to the repo root.
_REPO_ROOT = Path(__file__).parents[3]


def _load_corpus() -> list[dict[str, Any]]:
    raw = yaml.safe_load(_CORPUS_PATH.read_text())
    return [c for c in raw["cases"] if not c.get("skip", False)]


def _detect_mapping(case: dict[str, Any]) -> tuple[dict[str, str], str]:
    """Run the channel's detector on the fixture; return (mapping, tier)."""
    channel = case["channel"]
    if channel == "tabular":
        import polars as pl

        from moneybin.extractors.confidence import tier_for
        from moneybin.extractors.tabular.column_mapper import map_columns

        fixture_path = _REPO_ROOT / case["fixture_path"]
        df = pl.read_csv(fixture_path)
        result = map_columns(df)
        return result.field_mapping, tier_for(result.score, t_high=0.90, t_med=0.70)
    if channel == "gsheet":
        pytest.skip("gsheet channel calibration TBD")
    if channel == "pdf":
        pytest.skip("pdf extractor not yet built")
    raise NotImplementedError(f"unknown calibration channel: {channel}")


@pytest.mark.unit
class TestCalibrationCorpus:
    """Per-tier field-exact precision against the corpus."""

    def test_high_tier_field_exact_precision_meets_bar(self) -> None:
        """`high` band must clear the precision bar before self-accept can open.

        The expected_mapping in corpus.yaml covers only the required fields
        that must be present in a `high`-tier result. A case is "exact" if
        every key in expected_mapping resolves to the correct source column.
        Additional detected fields beyond expected_mapping are allowed.
        """
        corpus = _load_corpus()
        high_case_ids: list[str] = []
        high_exact = 0
        for case in corpus:
            detected, tier = _detect_mapping(case)
            if tier != "high":
                continue
            high_case_ids.append(case["id"])
            exact = all(
                detected.get(dest) == src
                for dest, src in case["expected_mapping"].items()
            )
            if exact:
                high_exact += 1
        if not high_case_ids:
            pytest.skip("no high-tier cases in corpus yet — extend the corpus")
        precision = high_exact / len(high_case_ids)
        assert precision >= _PRECISION_BAR, (
            f"high-tier field-exact precision {precision:.3f} below bar "
            f"{_PRECISION_BAR} across {len(high_case_ids)} cases: {high_case_ids}. "
            "Do not enable self_accept_high until this passes."
        )


@pytest.mark.unit
class TestSelfAcceptHighGate:
    """Lock the default-off behavior until calibration earns the switch."""

    def test_self_accept_high_default_remains_off(self) -> None:
        """ImportSettings().self_accept_high defaults False.

        Req 10 + Req 12 carve-out: tiered agent self-accept is gated until
        calibration proves the `high` band earns it. To enable it, you must:

        1. Extend the calibration corpus until
           TestCalibrationCorpus.test_high_tier_field_exact_precision_meets_bar
           passes on real-world data.
        2. Flip this test's expectation deliberately (with a CHANGELOG entry).
        """
        assert ImportSettings().self_accept_high is False
