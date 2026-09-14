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

Idempotent: DROP TABLE IF EXISTS is a no-op when the table is already absent
-- including on every fresh install, since its schema DDL file is removed in
the same change and init_schemas never creates it.
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop raw.ofx_institutions."""
    conn.execute("DROP TABLE IF EXISTS raw.ofx_institutions")  # type: ignore[union-attr]  # allowlisted literal
    logger.info("V064: dropped raw.ofx_institutions")
