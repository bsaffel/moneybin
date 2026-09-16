"""Drop the retired raw.ofx_institutions table (MB-256).

Its primary key is (organization, fid) -- no source_file, no import_id.
Every OFX import writes with on_conflict="upsert", so a second import from an
already-known institution re-stamps that shared row with its own import_id.
ImportService.revert_confirmed deletes purely by `WHERE import_id = ?`, so
reverting that second import deleted the row a still-complete first import
from the same institution needed.

Nothing reads the table: no SQLMesh model beyond the retired
prep.stg_ofx__institutions joins it, and core.dim_accounts already resolves
institution name, slug, and FID from prep.stg_ofx__accounts (institution_org /
institution_fid, sourced from raw.ofx_accounts) joined to seeds.institutions.
Every (organization, fid) pair the table ever held is already duplicated on
raw.ofx_accounts, so dropping it loses no data --
`SELECT DISTINCT institution_org, institution_fid FROM raw.ofx_accounts`
reproduces it.

Does NOT drop the orphaned prep.stg_ofx__institutions view, even though its
model file is deleted in the same change and a database that already
materialized it via `sqlmesh plan`/`transform apply` keeps the physical view
registered in DuckDB's catalog -- dropping the underlying table does not drop
a dependent view, so `build_live_catalog()` keeps advertising a view that
raises `CatalogException` on every `sql_query` SELECT until a later SQLMesh
plan prunes it. Confirmed real, but migrations may not touch it: `prep` is a
SQLMesh-owned schema (`sql/migrations/README.md` "Scope",
`tests/moneybin/test_migration_schema_ownership.py`) -- any migration
`DROP`/`ALTER`/etc. against it is a static-analysis CI failure by design,
because on a materialized database that relation is SQLMesh's, not the
migration ladder's, to manage. This is not a gap this migration introduces:
no earlier migration has ever retired a `prep.*`/`core.*` model either, and
the same transient window exists for any future model deletion. Closing it
needs a decision at the `sql_query`/`schema_catalog` layer (or a normal
`transform apply` after upgrade), not a migration -- out of scope here.

Also reconciles V003's stored checksum. V003's `_TABLE_COLUMNS` dropped its
`raw.ofx_institutions` entry in this same change -- this table's schema DDL
file is deleted, so a fresh install never creates the table and V003's
`ALTER TABLE raw.ofx_institutions ...` would raise `CatalogException` on
every fresh install if the entry stayed. That edit changes V003's file
checksum, so a database that already applied the old V003 body carries a
stale checksum in `app.schema_migrations` and `check_drift()` would report a
false "modified since it was applied" warning without this. Recomputes the
checksum from V003's current on-disk content and overwrites the stored row
-- a no-op on a fresh install, where V003 just ran with the same file.

Idempotent: DROP TABLE IF EXISTS is a no-op when the table is already absent
-- including on every fresh install, since its schema DDL file is removed in
the same change and init_schemas never creates it. The V003 checksum update
is idempotent by construction (recomputed from the current file every run)
and a no-op when no version-3 row exists yet.
"""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Deliberately hardcoded — migrations are frozen historical artifacts and
# must not import live module constants (e.g. moneybin.migrations.short_hash)
# whose meaning could shift. Matches V013's precedent for the same reason.
_MIGRATIONS_DIR = Path(__file__).resolve().parent
_V003_FILENAME = "V003__ofx_import_batch_columns.py"


def migrate(conn: object) -> None:
    """Drop raw.ofx_institutions; reconcile V003's checksum."""
    conn.execute("DROP TABLE IF EXISTS raw.ofx_institutions")  # type: ignore[union-attr]  # allowlisted literal
    logger.info("V064: dropped raw.ofx_institutions")

    v003_path = _MIGRATIONS_DIR / _V003_FILENAME
    if v003_path.exists():
        content = v003_path.read_bytes()
        checksum = hashlib.sha256(content).hexdigest()
        content_hash = checksum[:16]
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.schema_migrations SET checksum = ?, content_hash = ? "
            "WHERE version = 3",
            [checksum, content_hash],
        )
        logger.info("V064: reconciled V003 checksum after its ofx_institutions edit")
