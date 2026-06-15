"""V030: account-identity clean re-mint.

Pre-populates raw.* (ofx + tabular + plaid + manual) source accounts and
transactions, an accepted dedup match group, native-keyed ``app.*`` account
FKs, and curation rows keyed on the OLD ``transaction_id`` hash — then runs the
migration inside a BEGIN/COMMIT wrap (mirroring ``MigrationRunner``) and
verifies:

- every distinct source account got exactly one accepted ``source_native``
  link with a fresh 12-char canonical id + a paired ``account_link.insert``
  audit row (no cross-source collapse, no ``account_link_decisions``);
- every ``app.*`` ``account_id`` FK is re-pointed native → canonical (no native
  remains, no orphan) and each change is audited;
- ``app.transaction_id_aliases`` has an old→new row per re-keyed transaction and
  the curation FKs resolve through it;
- the OLD/NEW hashes the migration replays match an independently-derived
  formula, and DuckDB ``SUBSTRING(SHA256(x),1,16)`` == python
  ``hashlib.sha256(x).hexdigest()[:16]``.

Populated-fixture pattern per ``.claude/rules/database.md`` (V030 reshapes data).
"""

from __future__ import annotations

import hashlib

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V030__account_identity_remint import migrate
from tests.moneybin.migration_helpers import run_migration

# --- source accounts (native keys) -----------------------------------------
OFX_ORIGIN = "chase"
OFX_NATIVE = "ofx-acct-1"
CSV_ORIGIN = "chase_csv"
CSV_NATIVE = "csv-acct-1"
PLAID_ORIGIN = "item-xyz"
PLAID_NATIVE = "plaid-acct-1"
NATIVES = {OFX_NATIVE, CSV_NATIVE, PLAID_NATIVE}


def _h(text: str) -> str:
    """Independently-derived OLD/NEW truncated SHA-256 (mirrors the models)."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _old_single(source_type: str, stid: str, account_id: str) -> str:
    """OLD unmatched hash: SHA256(source_type|source_transaction_id|account_id)[:16]."""
    return _h(f"{source_type}|{stid}|{account_id}")


def _old_group(members: list[tuple[str, str, str]]) -> str:
    """OLD matched gold key: SHA256(LISTAGG(st|stid|aid sorted, '|'))[:16]."""
    parts = sorted(f"{st}|{stid}|{aid}" for (st, stid, aid) in members)
    return _h("|".join(parts))


def _new(source_type: str, origin: str, sak: str, stid: str) -> str:
    """NEW hash: SHA256(source_type|source_origin|source_account_key|stid)[:16]."""
    return _h(f"{source_type}|{origin}|{sak}|{stid}")


# --- OLD transaction ids the fixture's curation is keyed on ------------------
OLD_OFX1 = _old_single("ofx", "FIT001", OFX_NATIVE)
OLD_OFX2 = _old_single("ofx", "FIT002", OFX_NATIVE)
OLD_CSV = _old_single("csv", "csvhash001", CSV_NATIVE)
OLD_PLAID = _old_single("plaid", "plaidtxn001", PLAID_NATIVE)
OLD_MANUAL = _old_single("manual", "manual_001", OFX_NATIVE)
# Matched dedup group: two ofx rows in the same account.
OLD_GROUP = _old_group([("ofx", "FIT003", OFX_NATIVE), ("ofx", "FIT004", OFX_NATIVE)])

# --- NEW transaction ids expected after re-key ------------------------------
NEW_OFX1 = _new("ofx", OFX_ORIGIN, OFX_NATIVE, "FIT001")
NEW_OFX2 = _new("ofx", OFX_ORIGIN, OFX_NATIVE, "FIT002")
NEW_CSV = _new("csv", CSV_ORIGIN, CSV_NATIVE, "csvhash001")
NEW_PLAID = _new("plaid", PLAID_ORIGIN, PLAID_NATIVE, "plaidtxn001")
NEW_MANUAL = _new("manual", "user", OFX_NATIVE, "manual_001")
# Anchor of the ofx dedup group: both ofx (rank 0), tiebreak → FIT003 < FIT004.
NEW_GROUP = _new("ofx", OFX_ORIGIN, OFX_NATIVE, "FIT003")

EXPECTED_ALIASES = {
    OLD_OFX1: NEW_OFX1,
    OLD_OFX2: NEW_OFX2,
    OLD_CSV: NEW_CSV,
    OLD_PLAID: NEW_PLAID,
    OLD_MANUAL: NEW_MANUAL,
    OLD_GROUP: NEW_GROUP,
}


def _seed_raw(db: Database) -> None:
    """Insert source accounts + transactions across ofx/tabular/plaid/manual."""
    db.execute(
        "INSERT INTO raw.ofx_accounts "
        "(account_id, institution_org, source_file, extracted_at, source_type, source_origin) "
        "VALUES (?, 'Chase', 'a.ofx', CURRENT_TIMESTAMP, 'ofx', ?)",
        [OFX_NATIVE, OFX_ORIGIN],
    )
    for stid in ("FIT001", "FIT002", "FIT003", "FIT004"):
        db.execute(
            "INSERT INTO raw.ofx_transactions "
            "(source_transaction_id, account_id, amount, date_posted, source_file, "
            " source_type, source_origin, loaded_at) "
            "VALUES (?, ?, -10.00, TIMESTAMP '2025-01-01', 'a.ofx', 'ofx', ?, CURRENT_TIMESTAMP)",
            [stid, OFX_NATIVE, OFX_ORIGIN],
        )
    db.execute(
        "INSERT INTO raw.tabular_accounts "
        "(account_id, account_name, source_file, source_type, source_origin, import_id) "
        "VALUES (?, 'CSV Checking', 'a.csv', 'csv', ?, 'imp-1')",
        [CSV_NATIVE, CSV_ORIGIN],
    )
    db.execute(
        "INSERT INTO raw.tabular_transactions "
        "(transaction_id, account_id, transaction_date, amount, source_file, "
        " source_type, source_origin, import_id) "
        "VALUES ('csvhash001', ?, DATE '2025-01-02', -5.00, 'a.csv', 'csv', ?, 'imp-1')",
        [CSV_NATIVE, CSV_ORIGIN],
    )
    db.execute(
        "INSERT INTO raw.plaid_accounts "
        "(account_id, source_file, source_type, source_origin) "
        "VALUES (?, 'sync_1', 'plaid', ?)",
        [PLAID_NATIVE, PLAID_ORIGIN],
    )
    db.execute(
        "INSERT INTO raw.plaid_transactions "
        "(transaction_id, account_id, transaction_date, amount, source_file, "
        " source_type, source_origin) "
        "VALUES ('plaidtxn001', ?, DATE '2025-01-03', 7.50, 'sync_1', 'plaid', ?)",
        [PLAID_NATIVE, PLAID_ORIGIN],
    )
    db.execute(
        "INSERT INTO raw.manual_transactions "
        "(source_transaction_id, import_id, account_id, transaction_date, amount, "
        " description, created_by, transaction_id) "
        "VALUES ('manual_001', 'imp-m', ?, DATE '2025-01-04', -3.00, 'coffee', 'cli', ?)",
        [OFX_NATIVE, OLD_MANUAL],
    )


def _seed_match(db: Database) -> None:
    """Accepted dedup match joining FIT003 + FIT004 in the same ofx account."""
    db.execute(
        "INSERT INTO app.match_decisions "
        "(match_id, source_transaction_id_a, source_type_a, source_origin_a, "
        " source_transaction_id_b, source_type_b, source_origin_b, account_id, "
        " match_type, match_status, decided_by, decided_at) "
        "VALUES ('m-dedup', 'FIT003', 'ofx', ?, 'FIT004', 'ofx', ?, ?, "
        "        'dedup', 'accepted', 'auto', CURRENT_TIMESTAMP)",
        [OFX_ORIGIN, OFX_ORIGIN, OFX_NATIVE],
    )
    # A transfer match exercises account_id + account_id_b re-point (not a dedup
    # group — match_type='transfer' is excluded from the txn replay).
    db.execute(
        "INSERT INTO app.match_decisions "
        "(match_id, source_transaction_id_a, source_type_a, source_origin_a, "
        " source_transaction_id_b, source_type_b, source_origin_b, account_id, "
        " account_id_b, match_type, match_status, decided_by, decided_at) "
        "VALUES ('m-xfer', 'csvhash001', 'csv', ?, 'plaidtxn001', 'plaid', ?, ?, ?, "
        "        'transfer', 'accepted', 'auto', CURRENT_TIMESTAMP)",
        [CSV_ORIGIN, PLAID_ORIGIN, CSV_NATIVE, PLAID_NATIVE],
    )


def _seed_account_fks(db: Database) -> None:
    """Native-keyed app.* account FK rows (≥3 each where the table is affected)."""
    for native in NATIVES:
        db.execute(
            "INSERT INTO app.account_settings (account_id, display_name) VALUES (?, ?)",
            [native, f"label-{native}"],
        )
        db.execute(
            "INSERT INTO app.balance_assertions (account_id, assertion_date, balance) "
            "VALUES (?, DATE '2025-01-15', 100.00)",
            [native],
        )
    # categorization_rules: two account-scoped + one global (account_id NULL).
    db.execute(
        "INSERT INTO app.categorization_rules "
        "(rule_id, name, merchant_pattern, category, account_id) VALUES "
        "('rul-1', 'r1', 'STARBUCKS', 'Food', ?), "
        "('rul-2', 'r2', 'AMAZON', 'Shopping', ?), "
        "('rul-3', 'r3', 'GLOBAL', 'Other', NULL)",
        [OFX_NATIVE, CSV_NATIVE],
    )
    for i, native in enumerate(NATIVES):
        db.execute(
            "INSERT INTO app.gsheet_connections "
            "(connection_id, spreadsheet_id, sheet_gid, sheet_name, workbook_name, "
            " adapter, account_id, column_mapping, header_signature) "
            "VALUES (?, ?, ?, 'Sheet1', 'WB', 'transactions', ?, '{}', '[]')",
            [f"conn-{i}", f"ss-{i}", i, native],
        )


def _seed_curation(db: Database) -> None:
    """Curation rows keyed on OLD transaction ids (≥3 per affected table)."""
    for txn, category, by in (
        (OLD_OFX1, "Food", "rule"),
        (OLD_GROUP, "Dining", "user"),  # unique category → targets the matched group
        (OLD_CSV, "Shopping", "ai"),
        (OLD_MANUAL, "Coffee", "user"),
    ):
        db.execute(
            "INSERT INTO app.transaction_categories (transaction_id, category, categorized_by) "
            "VALUES (?, ?, ?)",
            [txn, category, by],
        )
    for i, txn in enumerate((OLD_OFX2, OLD_PLAID, OLD_GROUP)):
        db.execute(
            "INSERT INTO app.transaction_notes (note_id, transaction_id, text, author) "
            "VALUES (?, ?, 'note', 'cli')",
            [f"note-{i}", txn],
        )
    for i, txn in enumerate((OLD_OFX1, OLD_CSV, OLD_PLAID)):
        db.execute(
            "INSERT INTO app.transaction_tags (transaction_id, tag, applied_by) "
            "VALUES (?, ?, 'cli')",
            [txn, f"tag{i}"],
        )
    for i, txn in enumerate((OLD_GROUP, OLD_OFX1, OLD_MANUAL)):
        db.execute(
            "INSERT INTO app.transaction_splits "
            "(split_id, transaction_id, amount, ord, created_by) VALUES (?, ?, -1.00, ?, 'cli')",
            [f"split-{i}", txn, i],
        )


@pytest.fixture()
def v030_db(db: Database) -> Database:
    """Pre-migration DB: raw sources, a dedup match, native FKs, OLD-keyed curation."""
    _seed_raw(db)
    _seed_match(db)
    _seed_account_fks(db)
    _seed_curation(db)
    return db


def _canonicals(db: Database) -> set[str]:
    return {
        r[0]
        for r in db.execute(
            "SELECT account_id FROM app.account_links "
            "WHERE status = 'accepted' AND ref_kind = 'source_native'"
        ).fetchall()
    }


class TestV030AccountRemint:
    """Account re-mint: one canonical per source account, audited, no collapse."""

    def test_one_accepted_link_per_source_account(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        rows = v030_db.execute(
            "SELECT source_type, source_origin, ref_value, account_id, decided_by, status "
            "FROM app.account_links WHERE ref_kind = 'source_native'"
        ).fetchall()
        assert len(rows) == 3  # ofx + csv + plaid, no collapse
        by_native = {r[2]: r for r in rows}
        assert set(by_native) == NATIVES
        for r in rows:
            assert r[4] == "system" and r[5] == "accepted"
            assert len(r[3]) == 12  # uuid4().hex[:12] canonical

    def test_canonicals_are_distinct_no_collapse(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        assert len(_canonicals(v030_db)) == 3

    def test_each_link_has_paired_audit_row(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        link_ids = {
            r[0]
            for r in v030_db.execute("SELECT link_id FROM app.account_links").fetchall()
        }
        audited = {
            r[0]
            for r in v030_db.execute(
                "SELECT target_id FROM app.audit_log "
                "WHERE action = 'account_link.insert' AND actor = 'system' "
                "AND target_schema = 'app' AND target_table = 'account_links'"
            ).fetchall()
        }
        assert link_ids == audited and len(link_ids) == 3

    def test_no_account_link_decisions_written(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        assert v030_db.execute(
            "SELECT COUNT(*) FROM app.account_link_decisions"
        ).fetchone() == (0,)


class TestV030AccountFkRepoint:
    """Every app.* account_id FK is re-pointed native → canonical, no orphans."""

    @pytest.mark.parametrize(
        ("table", "column"),
        [
            ("account_settings", "account_id"),
            ("balance_assertions", "account_id"),
            ("categorization_rules", "account_id"),
            ("gsheet_connections", "account_id"),
            ("match_decisions", "account_id"),
            ("match_decisions", "account_id_b"),
        ],
    )
    def test_no_native_id_remains(
        self, v030_db: Database, table: str, column: str
    ) -> None:
        run_migration(v030_db, migrate)
        leftover = v030_db.execute(
            f"SELECT {column} FROM app.{table} "  # noqa: S608  # test constants
            f"WHERE {column} IN ('{OFX_NATIVE}', '{CSV_NATIVE}', '{PLAID_NATIVE}')"
        ).fetchall()
        assert leftover == []

    @pytest.mark.parametrize(
        ("table", "column"),
        [
            ("account_settings", "account_id"),
            ("balance_assertions", "account_id"),
            ("categorization_rules", "account_id"),
            ("gsheet_connections", "account_id"),
            ("match_decisions", "account_id"),
            ("match_decisions", "account_id_b"),
        ],
    )
    def test_every_non_null_fk_is_a_known_canonical(
        self, v030_db: Database, table: str, column: str
    ) -> None:
        run_migration(v030_db, migrate)
        canonicals = _canonicals(v030_db)
        values = {
            r[0]
            for r in v030_db.execute(
                f"SELECT {column} FROM app.{table} WHERE {column} IS NOT NULL"  # noqa: S608  # constants
            ).fetchall()
        }
        assert values <= canonicals

    def test_global_rule_account_id_stays_null(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        row = v030_db.execute(
            "SELECT account_id FROM app.categorization_rules WHERE rule_id = 'rul-3'"
        ).fetchone()
        assert row == (None,)

    def test_account_repoint_is_audited(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        # 3 account_settings + 3 balance_assertions + 2 rules + 3 gsheet
        # + 1 match account_id (dedup) + 1 match account_id + 1 account_id_b (xfer).
        assert v030_db.execute(
            "SELECT COUNT(*) FROM app.audit_log "
            "WHERE action = 'account.remint' AND actor = 'system'"
        ).fetchone() == (14,)


class TestV030TransactionRekey:
    """Curation re-key: aliases seeded, FKs resolve to NEW ids, audited."""

    def test_aliases_seeded_old_to_new(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        aliases = dict(
            v030_db.execute(
                "SELECT old_transaction_id, new_transaction_id FROM app.transaction_id_aliases"
            ).fetchall()
        )
        assert aliases == EXPECTED_ALIASES

    @pytest.mark.parametrize(
        "table",
        [
            "transaction_categories",
            "transaction_notes",
            "transaction_tags",
            "transaction_splits",
        ],
    )
    def test_no_old_transaction_id_remains(self, v030_db: Database, table: str) -> None:
        run_migration(v030_db, migrate)
        olds = tuple(EXPECTED_ALIASES)
        placeholders = ", ".join("?" * len(olds))
        leftover = v030_db.execute(
            f"SELECT transaction_id FROM app.{table} "  # noqa: S608  # constants
            f"WHERE transaction_id IN ({placeholders})",
            list(olds),
        ).fetchall()
        assert leftover == []

    @pytest.mark.parametrize(
        "table",
        [
            "transaction_categories",
            "transaction_notes",
            "transaction_tags",
            "transaction_splits",
        ],
    )
    def test_curation_resolves_through_alias(
        self, v030_db: Database, table: str
    ) -> None:
        run_migration(v030_db, migrate)
        # Every curation transaction_id is now a known NEW id.
        new_ids = set(EXPECTED_ALIASES.values())
        values = {
            r[0]
            for r in v030_db.execute(
                f"SELECT transaction_id FROM app.{table}"  # noqa: S608  # constants
            ).fetchall()
        }
        assert values <= new_ids

    def test_matched_group_curation_points_to_anchor(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        # The category seeded on the OLD group gold key now points to the anchor.
        row = v030_db.execute(
            "SELECT transaction_id FROM app.transaction_categories WHERE category = 'Dining'"
        ).fetchone()
        assert row == (NEW_GROUP,)

    def test_manual_raw_prediction_resynced(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        row = v030_db.execute(
            "SELECT transaction_id FROM raw.manual_transactions "
            "WHERE source_transaction_id = 'manual_001'"
        ).fetchone()
        assert row == (NEW_MANUAL,)

    def test_rekey_is_audited(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        # 4 categories + 3 notes + 3 tags + 3 splits all re-keyed.
        assert v030_db.execute(
            "SELECT COUNT(*) FROM app.audit_log "
            "WHERE action = 'transaction.rekey' AND actor = 'system'"
        ).fetchone() == (13,)

    def test_each_alias_has_paired_audit_row(self, v030_db: Database) -> None:
        """Seeding app.transaction_id_aliases is an app.* mutation → must be audited.

        Without the paired audit, `system doctor` audit-coverage (B9) flags every
        seeded alias as an orphaned mutation post-migration (Invariant 10).
        """
        run_migration(v030_db, migrate)
        alias_ids = {
            r[0]
            for r in v030_db.execute(
                "SELECT old_transaction_id FROM app.transaction_id_aliases"
            ).fetchall()
        }
        audited_ids = {
            r[0]
            for r in v030_db.execute(
                "SELECT target_id FROM app.audit_log "
                "WHERE action = 'transaction_id_alias.insert' AND actor = 'system'"
            ).fetchall()
        }
        assert alias_ids and alias_ids == audited_ids


class TestV030HashEquivalenceAndIdempotency:
    """Cross-checks: DuckDB==python hash, and replay safety."""

    def test_duckdb_substring_sha256_matches_python(self, v030_db: Database) -> None:
        probe = f"ofx|FIT001|{OFX_NATIVE}"
        duck = v030_db.execute("SELECT SUBSTRING(SHA256(?), 1, 16)", [probe]).fetchone()
        assert duck == (_h(probe),) == (OLD_OFX1,)

    def test_matched_gold_key_orders_by_tuple_not_string(self, db: Database) -> None:
        """OLD gold-key LISTAGG order must match the model's ORDER BY tuple.

        Two tabular rows in one account with stids 'x' and 'x0' — 'x' is a prefix
        of 'x0', and '0' (0x30) < '|' (0x7c), so sorting the concatenated
        ``st|stid|aid`` strings ('csv|x0|...' < 'csv|x|...') diverges from the
        model's ``ORDER BY source_type, source_transaction_id, account_id``
        ('x' < 'x0'). Expected OLD gold key comes from DuckDB's real LISTAGG
        (ground truth, independent of the migration's Python replay).
        """
        for stid in ("x", "x0"):  # equal loaded_at → anchor tiebreaks on stid → 'x'
            db.execute(
                "INSERT INTO raw.tabular_transactions "
                "(transaction_id, account_id, transaction_date, amount, source_file, "
                " source_type, source_origin, import_id, loaded_at) "
                "VALUES (?, ?, DATE '2025-02-01', -1.00, 'p.csv', 'csv', ?, 'imp-p', "
                "        TIMESTAMP '2025-02-01 00:00:00')",
                [stid, CSV_NATIVE, CSV_ORIGIN],
            )
        db.execute(
            "INSERT INTO app.match_decisions "
            "(match_id, source_transaction_id_a, source_type_a, source_origin_a, "
            " source_transaction_id_b, source_type_b, source_origin_b, account_id, "
            " match_type, match_status, decided_by, decided_at) "
            "VALUES ('m-pipe', 'x', 'csv', ?, 'x0', 'csv', ?, ?, "
            "        'dedup', 'accepted', 'auto', CURRENT_TIMESTAMP)",
            [CSV_ORIGIN, CSV_ORIGIN, CSV_NATIVE],
        )
        # Ground-truth OLD gold key via DuckDB's real LISTAGG ORDER BY tuple.
        expected_old = db.execute(
            "SELECT SUBSTRING(SHA256(LISTAGG(s || '|' || t || '|' || a, '|' "
            "  ORDER BY s, t, a)), 1, 16) "
            "FROM (VALUES ('csv', 'x', ?), ('csv', 'x0', ?)) v(s, t, a)",
            [CSV_NATIVE, CSV_NATIVE],
        ).fetchone()
        assert expected_old is not None
        expected_new = _new("csv", CSV_ORIGIN, CSV_NATIVE, "x")  # anchor = 'x'

        db.execute("BEGIN TRANSACTION")
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
        db.execute("COMMIT")

        row = db.execute(
            "SELECT new_transaction_id FROM app.transaction_id_aliases "
            "WHERE old_transaction_id = ?",
            [expected_old[0]],
        ).fetchone()
        assert row == (expected_new,)

    def test_idempotent_replay(self, v030_db: Database) -> None:
        run_migration(v030_db, migrate)
        canon_first = _canonicals(v030_db)
        run_migration(v030_db, migrate)
        # No new links, no duplicate aliases, canonicals unchanged.
        assert _canonicals(v030_db) == canon_first
        assert v030_db.execute("SELECT COUNT(*) FROM app.account_links").fetchone() == (
            3,
        )
        assert v030_db.execute(
            "SELECT COUNT(*) FROM app.transaction_id_aliases"
        ).fetchone() == (len(EXPECTED_ALIASES),)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
