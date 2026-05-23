"""Every concrete repo declares undo metadata (table_ref + pk_columns).

REC-PR3 Phase 3: the generic ``BaseRepo.undo_event`` (Phase 4) and the dispatch
registry (Phase 5) resolve a repo's table + primary key from these class attrs.
``__init_subclass__`` enforces their presence at class-definition; this test
adds correctness — every declared ``pk_columns`` matches the table's real
PRIMARY KEY in the live catalog, so a typo can't silently break undo targeting.
"""

from __future__ import annotations

from moneybin.database import Database
from moneybin.repositories import concrete_repo_classes
from moneybin.tables import TableRef


def test_repos_discovered() -> None:
    classes = concrete_repo_classes()
    names = {c.__name__ for c in classes}
    assert "TransactionNotesRepo" in names
    assert "TransactionTagsRepo" in names
    assert "TransactionSplitsRepo" in names
    # 14 original + 3 new repo-ified curation tables.
    assert len(classes) >= 17, f"only discovered {sorted(names)}"


def test_all_repos_declare_metadata() -> None:
    for cls in concrete_repo_classes():
        table_ref = getattr(cls, "table_ref", None)
        assert isinstance(table_ref, TableRef), f"{cls.__name__} missing table_ref"
        pk_columns = getattr(cls, "pk_columns", None)
        assert pk_columns and all(isinstance(c, str) for c in pk_columns), (
            f"{cls.__name__} missing/invalid pk_columns"
        )


def test_pk_columns_match_catalog(db: Database) -> None:
    for cls in concrete_repo_classes():
        ref = cls.table_ref
        row = db.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE schema_name = ? AND table_name = ? "
            "AND constraint_type = 'PRIMARY KEY'",
            [ref.schema, ref.name],
        ).fetchone()
        assert row is not None, f"{ref.full_name} has no PRIMARY KEY constraint"
        assert set(row[0]) == set(cls.pk_columns), (
            f"{cls.__name__}.pk_columns {cls.pk_columns} != catalog PK {row[0]}"
        )
