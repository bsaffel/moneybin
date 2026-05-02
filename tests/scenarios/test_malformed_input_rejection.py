"""Scenario: malformed input fixtures must raise a clear error, not silently load.

tiers: T1, T3-malformed-input.
"""

from __future__ import annotations

import pytest

from tests.scenarios._harnesses import assert_malformed_input_rejected
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec, Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env


def _malformed_scenario() -> Scenario:
    return Scenario(
        scenario="malformed-input-rejection",
        setup=SetupSpec(persona="basic", seed=42, years=1),
        pipeline=[],
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_malformed_csv_missing_amount_column() -> None:
    scenario = _malformed_scenario()
    spec = FixtureSpec(
        path="malformed/missing_amount.csv",
        account="bad-card",
        source_type="csv",
    )

    with scenario_env(scenario) as (db, _tmp, _env):
        result = assert_malformed_input_rejected(
            run=lambda: load_fixture_into_db(db, spec),
            expected_message_substring="amount",
        )
    result.raise_if_failed()


@pytest.mark.scenarios
@pytest.mark.slow
def test_malformed_ofx_missing_required_columns() -> None:
    # Truncated OFX is missing both `payee` and `transaction_type`. The
    # loader's select hits `transaction_type` first, so that's the column
    # named in the polars ColumnNotFoundError. Asserting on the column name
    # uniquely identifies the missing-column failure mode versus a generic
    # parse error.
    scenario = _malformed_scenario()
    spec = FixtureSpec(
        path="malformed/truncated.ofx.csv",
        account="bad-card",
        source_type="ofx",
    )

    with scenario_env(scenario) as (db, _tmp, _env):
        result = assert_malformed_input_rejected(
            run=lambda: load_fixture_into_db(db, spec),
            expected_message_substring="transaction_type",
        )
    result.raise_if_failed()
