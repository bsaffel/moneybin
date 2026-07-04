"""Validation contract for the seeds.category_source_map seed CSV."""

import csv
import pathlib

SEED = pathlib.Path("sqlmesh/models/seeds/category_source_map.csv")
REF = pathlib.Path("tests/moneybin/fixtures/plaid_pfc_v2_taxonomy.csv")
CATS = pathlib.Path("sqlmesh/models/seeds/categories.csv")


def _rows(p: pathlib.Path) -> list[dict[str, str]]:
    return list(csv.DictReader(p.open()))


def test_every_code_is_real_plaid():
    ref = _rows(REF)
    valid = {r["DETAILED"] for r in ref} | {r["PRIMARY"] for r in ref}
    for r in _rows(SEED):
        assert r["source_category_code"] in valid, (
            f"unknown Plaid code: {r['source_category_code']}"
        )


def test_code_level_matches_taxonomy():
    ref = _rows(REF)
    primaries = {r["PRIMARY"] for r in ref}
    for r in _rows(SEED):
        expect = "primary" if r["source_category_code"] in primaries else "detailed"
        assert r["code_level"] == expect, (
            f"{r['source_category_code']} level {r['code_level']} != {expect}"
        )


def test_reverse_lookup_is_deterministic():
    # exactly one category per (source_type, source_category_code)
    seen = {}
    for r in _rows(SEED):
        key = (r["source_type"], r["source_category_code"])
        assert key not in seen, f"duplicate mapping for {key}"
        seen[key] = r["category_id"]


def test_every_category_id_exists():
    cat_ids = {r["category_id"] for r in _rows(CATS)}
    for r in _rows(SEED):
        assert r["category_id"] in cat_ids, f"orphan category_id: {r['category_id']}"
