"""Deterministic gold key generation for transaction dedup.

Gold keys are SHA-256 hashes truncated to 16 hex characters (64 bits),
consistent with the content-hash ID strategy used elsewhere in MoneyBin.

Unmatched records: SHA-256(source_type|source_origin|source_account_key|source_transaction_id)
Matched groups: SHA-256(sorted pipe-delimited contributing tuples)
"""

import hashlib


def gold_key_unmatched(
    source_type: str,
    source_origin: str,
    source_account_key: str,
    source_transaction_id: str,
) -> str:
    """Generate a gold key for an unmatched (single-source) record."""
    raw = f"{source_type}|{source_origin}|{source_account_key}|{source_transaction_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def gold_key_matched(
    tuples: list[tuple[str, str, str]],
) -> str:
    """Generate a gold key for a matched group of source records.

    The tuples are sorted before hashing so the key is insertion-order
    independent.
    """
    sorted_tuples = sorted(tuples)
    raw = "|".join(f"{st}|{stid}|{aid}" for st, stid, aid in sorted_tuples)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
