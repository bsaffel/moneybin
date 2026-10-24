"""Source receipts participate in refresh freshness even without batch rows."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from moneybin.database import Database
from moneybin.services.transform_service import TransformService
from tests.moneybin.db_helpers import create_core_tables, record_sqlmesh_apply
from tests.moneybin.test_extractors.test_simplefin_extractor import load, source


def test_zero_batch_receipt_makes_existing_transform_pending(
    db: Database, declare_only_models: Callable[..., None]
) -> None:
    db.execute("SET TimeZone = 'UTC'")
    create_core_tables(db)
    declare_only_models("core.dim_accounts")
    record_sqlmesh_apply(db, datetime(2020, 1, 1))
    assert TransformService(db).freshness().pending is False
    data = source()
    data.batches = []
    load(db, data, status="failed")
    assert db.execute("SELECT count(*) FROM raw.simplefin_accounts").fetchone() == (0,)
    assert TransformService(db).freshness().pending is True
    record_sqlmesh_apply(
        db, datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=1)
    )
    assert TransformService(db).freshness().pending is False
    load(db, data, status="failed")
    assert TransformService(db).freshness().pending is False
