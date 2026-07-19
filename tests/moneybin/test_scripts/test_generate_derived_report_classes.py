"""Tests for scripts.generate_derived_report_classes: the codegen for _derived_classes.py.

`_render` had no test at all before this file — formatting/structure changes
could desync the checked-in module silently. These tests compile and exec the
rendered text (not just string-match it) so a change that produces syntactically
broken Python is caught directly, not inferred from a diff.
"""

from __future__ import annotations

import pytest

from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import DIM_ACCOUNTS, FCT_TRANSACTIONS
from scripts.generate_derived_report_classes import (
    _render,  # pyright: ignore[reportPrivateUsage]
)


def _exec_rendered(source: str) -> dict[str, object]:
    """Compile+exec rendered module source, returning its namespace."""
    namespace: dict[str, object] = {}
    exec(compile(source, "<rendered>", "exec"), namespace)  # noqa: S102  # test-only exec of generated codegen output
    return namespace


def test_render_empty_generated_produces_a_valid_empty_module() -> None:
    """No runner-less views left: the rendered module must still import cleanly.

    Regression for the reachable-empty-`generated` case: `_render` used to emit
    `from moneybin.tables import ` with nothing after it, which is a syntax
    error the moment every runner-less view gained a runner.
    """
    source = _render({})
    assert "from moneybin.tables import" not in source
    namespace = _exec_rendered(source)
    assert namespace["DERIVED_REPORT_CLASSES"] == {}


def test_render_nonempty_generated_produces_the_expected_dict() -> None:
    """A populated `generated` map renders an importable module with the same dict."""
    generated = {
        ("core", "fct_transactions"): {"amount": DataClass.TXN_AMOUNT},
    }
    source = _render(generated)
    assert "from moneybin.tables import FCT_TRANSACTIONS" in source
    namespace = _exec_rendered(source)
    assert namespace["DERIVED_REPORT_CLASSES"] == {
        (FCT_TRANSACTIONS.schema, FCT_TRANSACTIONS.name): {
            "amount": DataClass.TXN_AMOUNT
        }
    }


def test_render_multiple_tables_imports_and_emits_every_one() -> None:
    """Two runner-less views render as two entries with a combined import line.

    Dormant today — `net_worth` is the only runner-less view, so the loop and
    the `', '.join(const_names)` import never run with more than one table. It
    activates silently the moment a second runner-less view is added, which is
    exactly when a codegen bug would land uncaught. Asserting on the exec'd
    dict (not the text) also pins that neither entry is dropped or overwritten.
    """
    generated = {
        ("core", "fct_transactions"): {"amount": DataClass.TXN_AMOUNT},
        ("core", "dim_accounts"): {
            "account_id": DataClass.RECORD_ID,
            "routing_number": DataClass.ROUTING_NUMBER,
        },
    }
    source = _render(generated)
    assert "from moneybin.tables import DIM_ACCOUNTS, FCT_TRANSACTIONS" in source
    namespace = _exec_rendered(source)
    assert namespace["DERIVED_REPORT_CLASSES"] == {
        (DIM_ACCOUNTS.schema, DIM_ACCOUNTS.name): {
            "account_id": DataClass.RECORD_ID,
            "routing_number": DataClass.ROUTING_NUMBER,
        },
        (FCT_TRANSACTIONS.schema, FCT_TRANSACTIONS.name): {
            "amount": DataClass.TXN_AMOUNT
        },
    }


def test_render_refuses_a_view_with_no_tableref_constant() -> None:
    """A derived view with no `moneybin.tables` constant halts generation.

    `_render` indexes `name_by_key[key]` unguarded further down, so without this
    check a missing constant would surface as a bare KeyError mid-render — or,
    worse, a partially written module. Dormant while every runner-less view has
    a constant; the branch is what a contributor hits on their first new one.
    """
    with pytest.raises(SystemExit) as ei:
        _render({("reports", "not_a_real_view"): {"x": DataClass.AGGREGATE}})
    message = str(ei.value)
    assert "No TableRef constant" in message
    assert "('reports', 'not_a_real_view')" in message
