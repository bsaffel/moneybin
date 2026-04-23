"""Header normalization and alias matching for column mapping.

The FIELD_ALIASES table maps each destination field to a ranked list of
normalized header strings. Headers from source files are normalized before
matching (lowercase, collapse whitespace, strip quotes, replace separators).
"""

import re

# All alias values MUST be pre-normalized (lowercase, single spaces, no
# quotes, no underscores/hyphens). The test suite enforces this invariant.
FIELD_ALIASES: dict[str, list[str]] = {
    # Required fields (must find all three or detection fails)
    "transaction_date": [
        "transaction date",
        "trans date",
        "date",
        "effective date",
        "trade date",
        "txn date",
    ],
    "amount": [
        "amount",
        "transaction amount",
        "trans amount",
        "net amount",
    ],
    "description": [
        "description",
        "payee",
        "merchant",
        "narrative",
        "transaction description",
        "details",
        "name",
    ],
    # Amount variants (detected as amount if no single amount column)
    "debit_amount": [
        "debit",
        "debit amount",
        "withdrawals",
        "withdrawal",
        "money out",
        "debit amt",
        "outflow",
    ],
    "credit_amount": [
        "credit",
        "credit amount",
        "deposits",
        "deposit",
        "money in",
        "credit amt",
        "inflow",
    ],
    # Optional transaction fields
    "post_date": [
        "post date",
        "posting date",
        "settlement date",
        "posted date",
        "settle date",
    ],
    "memo": [
        "memo",
        "notes",
        "additional info",
        "extended description",
        "full description",
    ],
    "category": ["category", "transaction category"],
    "subcategory": ["subcategory", "sub category"],
    "transaction_type": [
        "type",
        "transaction type",
        "trans type",
        "tran type",
    ],
    "status": ["status", "state", "transaction status", "cleared"],
    "check_number": [
        "check number",
        "check no",
        "check #",
        "cheque number",
        "check",
    ],
    "source_transaction_id": [
        "transaction id",
        "trans id",
        "txn id",
        "transaction #",
        "fitid",
        "id",
        "unique id",
    ],
    "reference_number": [
        "reference",
        "ref",
        "confirmation",
        "conf number",
        "reference number",
        "ref number",
        "receipt",
    ],
    "balance": [
        "balance",
        "running balance",
        "available balance",
        "ledger balance",
    ],
    "currency": ["currency", "currency code", "ccy", "cur"],
    "member_name": [
        "member name",
        "account holder",
        "cardholder",
        "card member",
    ],
    # Account-identifying fields (trigger multi-account mode)
    "account_name": [
        "account",
        "account name",
        "acct name",
        "acct",
    ],
    "account_number": [
        "account #",
        "account number",
        "acct #",
        "acct number",
        "account no",
    ],
    "institution_name": [
        "institution",
        "bank",
        "bank name",
        "financial institution",
    ],
    "account_type": [
        "account type",
        "acct type",
        "class",
    ],
}

# Pre-built reverse lookup: normalized alias → destination field name.
# Built once at import time. First alias wins (earlier = higher priority).
_ALIAS_TO_FIELD: dict[str, str] = {}
for _field, _aliases in FIELD_ALIASES.items():
    for _alias in _aliases:
        if _alias not in _ALIAS_TO_FIELD:
            _ALIAS_TO_FIELD[_alias] = _field

# Fields that trigger multi-account mode when detected
ACCOUNT_IDENTIFYING_FIELDS: frozenset[str] = frozenset({
    "account_name",
    "account_number",
    "institution_name",
    "account_type",
})

# Required fields — detection fails if any of these can't be mapped
REQUIRED_FIELDS: frozenset[str] = frozenset({
    "transaction_date",
    "amount",
    "description",
})

_NORMALIZE_RE = re.compile(r"[\s_\-]+")
_QUOTE_RE = re.compile(r"""^["']|["']$""")


def normalize_header(header: str) -> str:
    """Normalize a column header for alias matching.

    Applies: lowercase, strip outer whitespace, strip quotes, replace
    underscores and hyphens with spaces, collapse multiple spaces.

    Args:
        header: Raw column header string from a source file.

    Returns:
        Normalized header string.
    """
    h = header.strip().lower()
    h = _QUOTE_RE.sub("", h)
    h = _NORMALIZE_RE.sub(" ", h)
    return h.strip()


def match_header_to_field(header: str) -> str | None:
    """Match a source column header to a destination field.

    Args:
        header: Raw column header string from a source file.

    Returns:
        Destination field name if matched, None otherwise.
    """
    normalized = normalize_header(header)
    return _ALIAS_TO_FIELD.get(normalized)
