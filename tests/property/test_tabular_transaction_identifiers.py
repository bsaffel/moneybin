"""Properties of tabular content-hash transaction identifiers.

The occurrence suffix protects distinct same-content transactions from staging
deduplication while retaining a re-importable bare identifier for the first
row. These expectations are derived from the identifier contract's specified
SHA-256 inputs, not from the transform implementation's counter.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from moneybin.extractors.tabular.transforms import transform_dataframe

_DATE = "01/15/2026"
_ACCOUNT_ID = "property-account"
D = Decimal


def _ids_for(rows: list[tuple[str, str, str]]) -> list[str]:
    """Run the public tabular transform without a source-provided ID column."""
    dataframe = pl.DataFrame({
        "Date": [row[0] for row in rows],
        "Amount": [row[1] for row in rows],
        "Description": [row[2] for row in rows],
    })
    result = transform_dataframe(
        df=dataframe,
        field_mapping={
            "transaction_date": "Date",
            "amount": "Amount",
            "description": "Description",
        },
        date_format="%m/%d/%Y",
        sign_convention="negative_is_expense",
        number_format="us",
        account_id=_ACCOUNT_ID,
        source_file="property.csv",
        source_type="csv",
        source_origin="property",
        import_id="property-import",
    )
    return result.transactions["transaction_id"].to_list()


@given(
    cents=st.integers(min_value=1, max_value=100_000),
    description=st.text(
        alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ", min_size=1, max_size=30
    ),
    duplicate_count=st.integers(min_value=1, max_value=8),
    unrelated_count=st.integers(min_value=0, max_value=8),
)
def test_content_hash_occurrences_keep_the_first_bare_and_make_every_twin_unique(
    cents: int,
    description: str,
    duplicate_count: int,
    unrelated_count: int,
) -> None:
    """N equal rows produce exactly the bare hash plus N - 1 occurrence hashes."""
    amount = f"-{D(cents) / D('100'):.2f}"
    repeated_row = (_DATE, amount, description)
    unrelated_rows = [
        ("01/14/2026", f"-{index + 1}.00", f"unrelated-{index}")
        for index in range(unrelated_count)
    ]
    rows = unrelated_rows + [repeated_row] * duplicate_count

    ids = _ids_for(rows)
    repeated_ids = ids[unrelated_count:]
    content_key = f"{_DATE}|{amount}|{description}|{_ACCOUNT_ID}"
    expected_ids = {
        "csv_"
        + hashlib.sha256(
            (content_key if occurrence == 0 else f"{content_key}|{occurrence}").encode()
        ).hexdigest()[:16]
        for occurrence in range(duplicate_count)
    }

    assert set(repeated_ids) == expected_ids
    assert len(repeated_ids) == len(set(repeated_ids))
    assert repeated_ids[0] == _content_hash_id(content_key)


@given(
    cents=st.integers(min_value=1, max_value=100_000),
    description=st.text(
        alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ", min_size=1, max_size=30
    ),
    duplicate_count=st.integers(min_value=2, max_value=8),
    unrelated_count=st.integers(min_value=1, max_value=8),
)
def test_inserting_unrelated_rows_does_not_rekey_any_content_occurrence(
    cents: int,
    description: str,
    duplicate_count: int,
    unrelated_count: int,
) -> None:
    """An unrelated file position cannot alter the repeated-content identifier set."""
    amount = f"-{D(cents) / D('100'):.2f}"
    repeated_rows = [(_DATE, amount, description)] * duplicate_count
    unrelated_rows = [
        ("01/14/2026", f"-{index + 1}.00", f"unrelated-{index}")
        for index in range(unrelated_count)
    ]

    ids_before = _ids_for(repeated_rows)
    ids_after = _ids_for(unrelated_rows + repeated_rows)[unrelated_count:]

    assert ids_after == ids_before


def _content_hash_id(content_key: str) -> str:
    """The contract's bare content-hash ID, derived independently of the transform."""
    return "csv_" + hashlib.sha256(content_key.encode()).hexdigest()[:16]
