"""Validation contract for the seeds.category_source_map seed CSV."""

import csv
import pathlib

from moneybin.database import SQLMESH_ROOT

_SEEDS = SQLMESH_ROOT / "models" / "seeds"
_FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures"

SEED = _SEEDS / "category_source_map.csv"
REF = _FIXTURES / "plaid_pfc_v2_taxonomy.csv"
CATS = _SEEDS / "categories.csv"
ORPHAN_ALLOWLIST = _FIXTURES / "category_orphan_allowlist.csv"
ROLLUP_ALLOWLIST = _FIXTURES / "category_rollup_allowlist.csv"

VALID_CLASSES = {"income", "expense", "transfer", "debt"}


def _rows(p: pathlib.Path) -> list[dict[str, str]]:
    return list(csv.DictReader(p.open()))


def _intentional_rollups() -> set[str]:
    """Plaid detailed codes deliberately left unmapped (they resolve to their primary)."""
    return {r["source_category_code"] for r in _rows(ROLLUP_ALLOWLIST)}


def _orphan_allowlist() -> set[str]:
    """Categories intentionally without a provider feed (rule/merchant-only)."""
    return {r["category_id"] for r in _rows(ORPHAN_ALLOWLIST)}


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


def test_every_category_has_valid_class():
    # Non-null class in the internal 4-class scheme (M1W).
    for r in _rows(CATS):
        assert r["class"] in VALID_CLASSES, (
            f"{r['category_id']} has class {r['class']!r} not in {sorted(VALID_CLASSES)}"
        )


def test_coverage_report_matches_intentional_rollups():
    # Enumerated coverage: the set of unmapped Plaid detailed codes must equal
    # the intentional-rollup allowlist exactly — catches an accidental gap (a new
    # code nobody mapped) AND a stale allowlist entry (a code since mapped/removed).
    mapped = {r["source_category_code"] for r in _rows(SEED)}
    detailed = {r["DETAILED"] for r in _rows(REF)}
    unmapped = detailed - mapped
    rollups = _intentional_rollups()
    missing = unmapped - rollups
    stale = rollups - unmapped
    assert not missing, (
        f"unmapped Plaid codes not on the roll-up allowlist: {sorted(missing)}"
    )
    assert not stale, (
        f"roll-up allowlist entries that are no longer unmapped: {sorted(stale)}"
    )


def test_no_unjustified_orphan_categories():
    # Every category with no bridge feed must be justified on the orphan allowlist,
    # and the allowlist may not carry stale (fed or non-existent) entries.
    cat_ids = {r["category_id"] for r in _rows(CATS)}
    fed = {r["category_id"] for r in _rows(SEED)}
    allow = _orphan_allowlist()
    unknown = allow - cat_ids
    assert not unknown, (
        f"orphan allowlist references unknown categories: {sorted(unknown)}"
    )
    stale_fed = allow & fed
    assert not stale_fed, (
        f"orphan allowlist entries that are actually fed: {sorted(stale_fed)}"
    )
    unjustified = (cat_ids - fed) - allow
    assert not unjustified, (
        f"orphan categories without an allowlist entry: {sorted(unjustified)}"
    )
