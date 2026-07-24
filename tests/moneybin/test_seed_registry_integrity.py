"""Integrity guards for the seed registries the staging views join against.

Both `seeds.account_type_map` and `seeds.institutions` are declared `grain alias`
/ `grain fid`, but a SEED model's grain is documentation — SQLMesh does not
enforce it. A duplicate key is therefore silent, and it does not merely pick a
winner: the staging views LEFT JOIN these registries, so two rows sharing a key
fan the join out and duplicate the account. These tests are cheap and read the
shipped CSVs directly, so they also cover the packaged-resource path.
"""

from __future__ import annotations

import csv
import io
from importlib import resources

import pytest

_SEEDS = "sqlmesh/models/seeds"


def _rows(filename: str) -> list[dict[str, str]]:
    raw = resources.files("moneybin").joinpath(f"{_SEEDS}/{filename}").read_text()
    return list(csv.DictReader(io.StringIO(raw)))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("filename", "key"),
    [("account_type_map.csv", "alias"), ("institutions.csv", "fid")],
)
def test_seed_keys_are_unique(filename: str, key: str) -> None:
    """A duplicate key fans out the staging join and duplicates the account."""
    keys = [row[key] for row in _rows(filename)]
    assert keys, f"{filename} is empty"
    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    assert not duplicates, f"{filename} has duplicate {key} values: {duplicates}"


@pytest.mark.unit
def test_account_type_map_aliases_are_uppercase() -> None:
    """Lookup is `m.alias = UPPER(...)`, so a lower-case alias can never match."""
    for row in _rows("account_type_map.csv"):
        assert row["alias"] == row["alias"].upper(), (
            f"alias {row['alias']!r} is not upper-case and is therefore unreachable"
        )


@pytest.mark.unit
def test_account_type_map_uses_the_canonical_vocabulary() -> None:
    """Every mapped type must be one of the five canonical values.

    The registry is the only thing standing between four source vocabularies and
    one; a typo here reintroduces the drift it exists to remove.
    """
    canonical = {"depository", "credit", "loan", "investment", "other"}
    for row in _rows("account_type_map.csv"):
        assert row["account_type"] in canonical, (
            f"alias {row['alias']!r} maps to {row['account_type']!r}, "
            f"which is outside the canonical set {sorted(canonical)}"
        )
