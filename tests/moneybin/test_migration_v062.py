"""Review-only Proposal migration is additive and replay-safe."""

import importlib

from moneybin.database import Database
from tests.moneybin.migration_helpers import run_migration


def test_proposal_migration_replays_without_changing_existing_state(
    db: Database,
) -> None:
    migrate = importlib.import_module(
        "moneybin.sql.migrations.V062__investment_match_proposals"
    ).migrate
    db.execute("DROP TABLE app.investment_match_decisions")
    source_before = db.execute(
        "SELECT * FROM raw.plaid_investment_transactions"
    ).fetchall()
    run_migration(db, migrate)
    db.execute("""INSERT INTO app.investment_match_decisions (
        proposal_id, algorithm_version, relationship_fingerprint,
        candidate_graph_fingerprint, confidence_band, status,
        auto_eligible, is_competing, proposal, actor
    ) VALUES ('proposal', 'v1', 'relationship', 'graph', 'exact', 'pending',
              TRUE, FALSE, '{}', 'test')""")
    before = db.execute("SELECT * FROM app.investment_match_decisions").fetchall()
    run_migration(db, migrate)
    assert (
        db.execute("SELECT * FROM app.investment_match_decisions").fetchall() == before
    )
    assert (
        db.execute("SELECT * FROM raw.plaid_investment_transactions").fetchall()
        == source_before
    )
