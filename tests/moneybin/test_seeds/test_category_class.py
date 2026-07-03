"""Validation contract for the accounting ``class`` column in categories.csv."""

from __future__ import annotations

import csv
import pathlib


def test_class_assignment() -> None:
    rows = list(
        csv.DictReader(pathlib.Path("sqlmesh/models/seeds/categories.csv").open())
    )

    def expect(cid: str) -> str:
        return (
            "income"
            if cid.startswith("INC")
            else "transfer"
            if cid.startswith("TRN")
            else "debt"
            if cid.startswith("LNP")
            else "expense"
        )

    for r in rows:
        assert r["class"] == expect(r["category_id"]), (
            f"{r['category_id']}: {r['class']}"
        )
    assert sum(r["class"] == "income" for r in rows) == 8
    assert sum(r["class"] == "debt" for r in rows) == 6
