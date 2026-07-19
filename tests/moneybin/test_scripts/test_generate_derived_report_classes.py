"""Tests for scripts.generate_derived_report_classes: the codegen for _derived_classes.py.

`_render` had no test at all before this file — formatting/structure changes
could desync the checked-in module silently. These tests compile and exec the
rendered text (not just string-match it) so a change that produces syntactically
broken Python is caught directly, not inferred from a diff.
"""

from __future__ import annotations

from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import FCT_TRANSACTIONS
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
