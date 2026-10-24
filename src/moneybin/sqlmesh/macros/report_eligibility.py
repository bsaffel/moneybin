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
    ``core.dim_accounts``. The net-worth and balance-drift models read balances,
    not transactions, so they keep their own archive rule and must not call this.

    Deliberately does not read ``core.fct_transactions.is_categorizable``; see
    ``docs/specs/categorizable-transactions.md``.
    """
    return f"NOT {transactions}.is_transfer AND NOT {accounts}.archived"
