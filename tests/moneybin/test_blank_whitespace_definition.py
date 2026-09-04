"""The SQL blank test means exactly what Python's ``str.strip()`` means.

Four sites carry the same character class: the two staging models that null
a blank category out on import, and V054 and V055, which backfill the same
rule onto stored splits, merchant defaults, and the categorizations a blank
merchant default produced. They exist because ``validate_category_text``
refuses a blank on the write path using ``str.strip()`` — if the SQL class and
``str.strip()`` disagree on any character, a value the validator calls blank
survives import as a non-NULL category and stays hidden from
``core.uncategorized_queue``, which is the whole defect this rule closes.

This test asserts the *rule*, not a sample of characters. Three rounds of
review each found one more character missing from a hand-written list — a tab,
then a non-breaking space, then U+0085 and the information separators —
because a list can only ever be patched by the character that just leaked.
Enumerating every codepoint Python calls whitespace makes the next omission
fail here instead of in review.

Each site is read from its own source text rather than imported, so all four
are checked the same way and a copy that drifts is named by the failure.

These tests open a bare in-memory ``duckdb.connect()`` instead of going through
``Database``, which AGENTS.md otherwise requires. What is under test is DuckDB's
own regex character class against Python's ``str.strip()``: no MoneyBin table,
schema, or row is involved, and the connection holds no data to protect.
``Database`` adds encryption and the crypto extension and disables extension
autoloading, none of which reaches RE2's handling of the core ``REGEXP_*``
built-ins, so routing through it would mean a keyed database per test without
changing a single answer.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SITES = (
    _REPO_ROOT / "src/moneybin/sqlmesh/models/prep/stg_tabular__transactions.sql",
    _REPO_ROOT / "src/moneybin/sqlmesh/models/prep/stg_manual__transactions.sql",
    _REPO_ROOT / "src/moneybin/sql/migrations/V054__backfill_blank_split_categories.py",
    _REPO_ROOT
    / "src/moneybin/sql/migrations/V055__backfill_blank_merchant_categories.py",
)

#: The character class each site uses to decide "entirely whitespace". Matches
#: the staging models' REGEXP_REPLACE anchor and the migrations' _BLANK
#: constant alike.
_CLASS_IN_SOURCE = re.compile(r"(\[\\p\{Z\}[^\]]*\])")


def _python_whitespace() -> list[int]:
    """Every codepoint ``str.strip()`` removes."""
    return [cp for cp in range(0x110000) if chr(cp).isspace()]


def _classes_in(site: Path) -> set[str]:
    found = set(_CLASS_IN_SOURCE.findall(site.read_text()))
    assert found, f"no blank character class found in {site.name}"
    return found


def _agreed_class() -> str:
    classes: set[str] = set()
    for site in _SITES:
        classes |= _classes_in(site)
    assert len(classes) == 1, f"sites disagree: {sorted(classes)}"
    return classes.pop()


def _matches(con: duckdb.DuckDBPyConnection, value: str, pattern: str) -> bool:
    row = con.execute("SELECT REGEXP_FULL_MATCH(?, ?)", [value, pattern]).fetchone()
    assert row is not None
    return bool(row[0])


def test_every_site_uses_the_same_character_class() -> None:
    """Three copies of one rule drift the moment they stop being identical."""
    per_site = {site.name: _classes_in(site) for site in _SITES}
    distinct: set[str] = set()
    for classes in per_site.values():
        distinct |= classes

    assert len(distinct) == 1, f"sites disagree: {per_site}"


def test_the_class_matches_str_strip_on_every_whitespace_codepoint() -> None:
    """The completeness half: nothing Python calls blank survives the class.

    Fails naming the exact codepoints missing, so a future gap reports itself
    instead of arriving as a review comment.
    """
    pattern = f"^{_agreed_class()}*$"
    con = duckdb.connect()

    missed = [cp for cp in _python_whitespace() if not _matches(con, chr(cp), pattern)]

    assert not missed, "class misses " + " ".join(f"U+{cp:04X}" for cp in missed)


@pytest.mark.parametrize(
    "value",
    ["Coffee", "Food & Drink", "  Gas  ", "日本語", "Food \xa0 Dining", "-", "0"],
)
def test_the_class_calls_nothing_else_blank(value: str) -> None:
    """The restraint half: a real category must never be classified blank."""
    con = duckdb.connect()

    assert not _matches(con, value, f"^{_agreed_class()}*$")


def test_trimming_agrees_with_str_strip() -> None:
    """Trim parity, not just the blank test: padding is stripped identically."""
    con = duckdb.connect()
    cls = _agreed_class()
    trim = f"^{cls}+|{cls}+$"
    samples = [
        "  Groceries  ",
        "\xa0\tGas\t\xa0",
        "　日本語　",
        "Food \xa0 Dining",
        "\x85Coffee\x1c",
        "".join(chr(cp) for cp in _python_whitespace()),
        "Coffee",
        "",
    ]

    for value in samples:
        row = con.execute(
            "SELECT REGEXP_REPLACE(?, ?, '', 'g')", [value, trim]
        ).fetchone()
        assert row is not None
        assert row[0] == value.strip(), (
            f"{value!r}: sql={row[0]!r} py={value.strip()!r}"
        )
