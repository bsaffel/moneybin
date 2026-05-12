"""Scenario: archive cascade flows from app.account_settings through core.dim_accounts.

Verifies the cascade reaches reports.net_worth and the accounts list CLI.

Fixture: tests/fixtures/ofx/multi_account_sample.ofx
  - CHECKING1 (CHECKING): balance $1,000.00, 1 debit (-$50.00)
  - SAVINGS1  (SAVINGS):  balance $5,100.00, 1 credit (+$100.00)

Expectations (independently derived from fixture file before running):
  - Pre-archive:  dim_accounts has 2 rows; both have archived=FALSE,
                  include_in_net_worth=TRUE.
  - Post-archive: CHECKING1 has archived=TRUE, include_in_net_worth=FALSE.
                  SAVINGS1 is unchanged.
  - reports.net_worth after second transform: CHECKING1 excluded;
                  account_count on the balance date must be 1 (SAVINGS1 only).
  - list_accounts(include_archived=False): 1 account (SAVINGS1).
  - list_accounts(include_archived=True):  2 accounts (both).
"""

from __future__ import annotations

import pytest

from moneybin.database import sqlmesh_context
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

# Models that read from app.account_settings and their dependents.
# SQLMesh's interval-based optimizer skips FULL-kind models if their intervals
# are already covered for the current day. After writing to app.account_settings
# (an external table, invisible to SQLMesh's dependency graph), we must
# explicitly restate these models so the updated archived/include_in_net_worth
# flags are picked up without waiting for the next calendar day.
_ARCHIVE_RESTATE_MODELS = [
    "core.dim_accounts",
    "core.fct_balances_daily",
]


@pytest.mark.scenarios
@pytest.mark.slow
def test_archive_cascade_excludes_from_networth() -> None:
    """Archiving an account flips include_in_net_worth and excludes from reports.net_worth."""
    # Bootstrap using the multi-account scenario's setup (import + transform pipeline).
    # We drive steps manually so we can inject the archive mutation between transforms.
    scenario = load_shipped_scenario("ofx-multi-account-statement")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        # Step 1: import the two-account OFX fixture.
        run_step("import_file", scenario.setup, db, env=env)

        # Step 2: first transform — materialise dim_accounts + fct_balances_daily.
        run_step("transform", scenario.setup, db, env=env)

        # --- Pre-archive assertions (derived from fixture) ---
        # Fixture has 2 STMTTRNRS blocks → 2 accounts in dim_accounts.
        pre_total = db.execute("SELECT COUNT(*) FROM core.dim_accounts").fetchone()
        assert pre_total is not None and pre_total[0] == 2, (
            f"Expected 2 accounts before archive, got {pre_total}"
        )

        # Both accounts start with archived=FALSE and include_in_net_worth=TRUE.
        pre_flags = db.execute(
            """
            SELECT account_id, archived, include_in_net_worth
            FROM core.dim_accounts
            ORDER BY account_id
            """
        ).fetchall()
        for row in pre_flags:
            acct_id, archived, include = row
            assert not archived, f"Account {acct_id} should not be archived pre-archive"
            assert include, (
                f"Account {acct_id} should have include_in_net_worth=TRUE pre-archive"
            )

        # reports.net_worth is a VIEW — verify both accounts contribute (account_count=2).
        pre_nw = db.execute(
            "SELECT account_count FROM reports.net_worth ORDER BY balance_date LIMIT 1"
        ).fetchone()
        assert pre_nw is not None and pre_nw[0] == 2, (
            f"Expected account_count=2 before archive, got {pre_nw}"
        )

        # Step 3: archive CHECKING1 via AccountService (the production code path).
        from moneybin.services.account_service import AccountService

        svc = AccountService(db)
        updated = svc.archive("CHECKING1")

        # Verify the service write is immediately reflected in app.account_settings
        # (before the next transform propagates it to dim_accounts).
        assert updated.archived is True
        assert updated.include_in_net_worth is False

        # Step 4: second transform with restate_models — forces dim_accounts to
        # re-run even though SQLMesh already covered today's interval on the first
        # transform. Without restate_models, SQLMesh's interval optimizer skips all
        # FULL-kind core models because intervals are already satisfied for today.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=_ARCHIVE_RESTATE_MODELS,
                auto_apply=True,
                no_prompts=True,
            )

        # --- Post-archive assertions ---

        # CHECKING1: archived=TRUE, include_in_net_worth=FALSE.
        checking = db.execute(
            """
            SELECT archived, include_in_net_worth
            FROM core.dim_accounts
            WHERE account_id = ?
            """,
            ["CHECKING1"],
        ).fetchone()
        assert checking is not None, "CHECKING1 must still exist in dim_accounts"
        checking_archived, checking_include = checking
        assert checking_archived is True, (
            "CHECKING1.archived must be TRUE after archive"
        )
        assert checking_include is False, (
            "CHECKING1.include_in_net_worth must be FALSE after archive (cascade)"
        )

        # SAVINGS1: unchanged — archived=FALSE, include_in_net_worth=TRUE.
        savings = db.execute(
            """
            SELECT archived, include_in_net_worth
            FROM core.dim_accounts
            WHERE account_id = ?
            """,
            ["SAVINGS1"],
        ).fetchone()
        assert savings is not None, "SAVINGS1 must still exist in dim_accounts"
        savings_archived, savings_include = savings
        assert savings_archived is False, "SAVINGS1.archived must remain FALSE"
        assert savings_include is True, "SAVINGS1.include_in_net_worth must remain TRUE"

        # reports.net_worth is a VIEW that re-evaluates on every read.
        # After archiving CHECKING1, only SAVINGS1 contributes → account_count=1.
        # (Derived independently: fixture has 1 non-archived account after the mutation.)
        post_nw = db.execute(
            "SELECT account_count FROM reports.net_worth ORDER BY balance_date LIMIT 1"
        ).fetchone()
        assert post_nw is not None and post_nw[0] == 1, (
            f"Expected account_count=1 after archive (SAVINGS1 only), got {post_nw}"
        )

        # AccountService.list_accounts() default hides archived → 1 result.
        default_list = svc.list_accounts(include_archived=False)
        assert len(default_list.accounts) == 1, (
            f"list_accounts() default: expected 1 account, got {len(default_list.accounts)}"
        )
        assert default_list.accounts[0]["account_id"] == "SAVINGS1", (
            f"Expected SAVINGS1 in default list, got {default_list.accounts[0]['account_id']}"
        )

        # list_accounts(include_archived=True) returns both accounts.
        full_list = svc.list_accounts(include_archived=True)
        assert len(full_list.accounts) == 2, (
            f"list_accounts(include_archived=True): expected 2, got {len(full_list.accounts)}"
        )
        ids = {row["account_id"] for row in full_list.accounts}
        assert ids == {"CHECKING1", "SAVINGS1"}, (
            f"Expected both accounts in --include-archived list, got {ids}"
        )
