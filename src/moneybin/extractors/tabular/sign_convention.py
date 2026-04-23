"""Sign convention inference for amount columns.

Determines how the source represents expenses vs income:
- negative_is_expense: negative = expense, positive = income (MoneyBin native)
- negative_is_income: inverted (credit card statements)
- split_debit_credit: separate debit and credit columns
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class SignConventionResult:
    """Result of sign convention inference."""

    convention: str
    """One of: negative_is_expense, negative_is_income, split_debit_credit."""

    needs_confirmation: bool = False
    """True if the convention is ambiguous and needs user confirmation."""

    reason: str = ""
    """Human-readable explanation of the inference."""


def infer_sign_convention(
    amount_values: list[str | None] | None,
    debit_values: list[str | None] | None,
    credit_values: list[str | None] | None,
    *,
    header_context: str = "",
) -> SignConventionResult:
    """Infer the sign convention from sample values.

    Args:
        amount_values: Values from a single amount column (if present).
        debit_values: Values from a debit column (if present).
        credit_values: Values from a credit column (if present).
        header_context: Lowercase header text for context clues
            (e.g., "credit" suggests credit card statement).

    Returns:
        SignConventionResult with the inferred convention.
    """
    # Split debit/credit columns
    if debit_values is not None and credit_values is not None:
        return SignConventionResult(
            convention="split_debit_credit",
            reason="Separate debit and credit columns detected",
        )

    if amount_values is None:
        return SignConventionResult(
            convention="negative_is_expense",
            needs_confirmation=True,
            reason="No amount column provided",
        )

    clean = [v.strip() for v in amount_values if v and v.strip()]
    if not clean:
        return SignConventionResult(
            convention="negative_is_expense",
            needs_confirmation=True,
            reason="No non-empty amount values",
        )

    has_negative = any(
        v.startswith("-") or (v.startswith("(") and v.endswith(")")) for v in clean
    )

    if not has_negative:
        return SignConventionResult(
            convention="negative_is_expense",
            needs_confirmation=True,
            reason="All amounts are positive — sign convention is ambiguous",
        )

    if "credit" in header_context.lower():
        return SignConventionResult(
            convention="negative_is_income",
            reason="Credit card context detected — negative values are payments/credits",
        )

    return SignConventionResult(
        convention="negative_is_expense",
        reason="Mixed positive/negative values — standard convention",
    )
