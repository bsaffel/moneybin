"""Shared predicates for ``reports.*`` models."""

from sqlmesh import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    macro,
)
from sqlmesh.core.macros import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    MacroEvaluator,
)


@macro()
def report_eligible_transaction(
    evaluator: MacroEvaluator, transactions: str, accounts: str
) -> str:
    """Transactions a spending or cash-flow report counts.

    Takes the model's aliases for ``core.fct_transactions`` and
    ``core.dim_accounts``. The net-worth and balance-drift models scope archived
    accounts by date instead and must not call this.
    """
    return f"NOT {transactions}.is_transfer AND NOT {accounts}.archived"
