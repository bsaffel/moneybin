"""Scenario: archiving flows from app.account_settings through core.dim_accounts.

Verifies archived/archived_at reach reports.net_worth and the accounts list
CLI. The archive cascade (archived=True forcing include_in_net_worth=False)
is retired — see docs/specs/reports-net-worth-sql-surface.md
§Prerequisites — so this scenario now asserts the two flags are independent
and that archived_at is stamped instead.

Fixture: tests/fixtures/ofx/multi_account_sample.ofx
  - CHECKING1 (CHECKING): balance $1,000.00, 1 debit (-$50.00)
  - SAVINGS1  (SAVINGS):  balance $5,100.00, 1 credit (+$100.00)

Expectations (independently derived from fixture file before running):
  - Pre-archive:  dim_accounts has 2 rows; both have archived=FALSE,
                  archived_at=NULL, include_in_net_worth=TRUE.
  - Post-archive: CHECKING1 has archived=TRUE, archived_at=today,
                  include_in_net_worth=TRUE (untouched -- no cascade).
                  SAVINGS1 is unchanged.
  - reports.net_worth is date-scoped (Requirement 9): an archived account is
    excluded only for balance_date > archived_at, never retroactively. The
    fixture's single balance_date (2026-01-31) precedes "today", so
    archiving via AccountService (which always stamps archived_at=today)
    does NOT exclude CHECKING1 on that date -- account_count stays 2. The
    exclusion side is exercised by backdating archived_at ahead of that
    balance_date (AccountService has no path to stamp a historical archive
    date, so this precondition is set directly): once archived_at precedes
    the balance_date, account_count drops to 1 (SAVINGS1 only).
  - list_accounts(include_archived=False): 1 account (SAVINGS1).
  - list_accounts(include_archived=True):  2 accounts (both).
"""

from __future__ import annotations

from datetime import date

import pytest

from moneybin.database import sqlmesh_context
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

# Models that read from app.account_settings and their dependents.
# SQLMesh's interval-based optimizer skips FULL-kind models if their intervals
# are already covered for the current day. After writing to app.account_settings
# (an external table, invisible to SQLMesh's dependency graph), we must
# explicitly restate these models so the updated archived/archived_at flags
# are picked up without waiting for the next calendar day.
_ARCHIVE_RESTATE_MODELS = [
    "core.dim_accounts",
    "core.fct_balances_daily",
]


@pytest.mark.scenarios
@pytest.mark.slow
def test_archive_excludes_from_networth_without_cascading_include() -> None:
    """Archiving excludes from reports.net_worth without touching include_in_net_worth."""
    # Bootstrap using the multi-account scenario's setup (import + transform pipeline).
    # We drive steps manually so we can inject the archive mutation between transforms.
    scenario = load_shipped_scenario("ofx-multi-account-statement")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        # Step 1: import the two-account OFX fixture.
        run_step("import_file", scenario.setup, db, env=env)

        # Step 2: first transform — materialise dim_accounts + fct_balances_daily.
        run_step("transform", scenario.setup, db, env=env)

        # The AccountResolver mints opaque canonical account_ids on import, so the
        # source-native ACCTIDs (CHECKING1/SAVINGS1) are no longer the dim_accounts
        # key. Resolve native ref → canonical id via app.account_links.
        links = dict(
            db.execute(
                "SELECT ref_value, account_id FROM app.account_links "
                "WHERE ref_kind = 'source_native' AND status = 'accepted'"
            ).fetchall()
        )
        checking_id = links["CHECKING1"]
        savings_id = links["SAVINGS1"]

        # --- Pre-archive assertions (derived from fixture) ---
        # Fixture has 2 STMTTRNRS blocks → 2 accounts in dim_accounts.
        pre_total = db.execute("SELECT COUNT(*) FROM core.dim_accounts").fetchone()
        assert pre_total is not None and pre_total[0] == 2, (
            f"Expected 2 accounts before archive, got {pre_total}"
        )

        # Both accounts start with archived=FALSE, archived_at=NULL,
        # include_in_net_worth=TRUE.
        pre_flags = db.execute(
            """
            SELECT account_id, archived, archived_at, include_in_net_worth
            FROM core.dim_accounts
            ORDER BY account_id
            """
        ).fetchall()
        for row in pre_flags:
            acct_id, archived, archived_at, include = row
            assert not archived, f"Account {acct_id} should not be archived pre-archive"
            assert archived_at is None, (
                f"Account {acct_id} should have archived_at=NULL pre-archive"
            )
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
        updated = svc.archive(checking_id)

        # Verify the service write is immediately reflected in app.account_settings
        # (before the next transform propagates it to dim_accounts).
        assert updated.archived is True
        assert updated.archived_at == date.today()
        # No cascade: include_in_net_worth stays at its default TRUE.
        assert updated.include_in_net_worth is True

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

        # CHECKING1: archived=TRUE, archived_at=today, include_in_net_worth=TRUE.
        checking = db.execute(
            """
            SELECT archived, archived_at, include_in_net_worth
            FROM core.dim_accounts
            WHERE account_id = ?
            """,
            [checking_id],
        ).fetchone()
        assert checking is not None, "CHECKING1 must still exist in dim_accounts"
        checking_archived, checking_archived_at, checking_include = checking
        assert checking_archived is True, (
            "CHECKING1.archived must be TRUE after archive"
        )
        assert checking_archived_at == date.today(), (
            "CHECKING1.archived_at must be stamped with today's date"
        )
        assert checking_include is True, (
            "CHECKING1.include_in_net_worth must stay TRUE — no cascade"
        )

        # SAVINGS1: unchanged — archived=FALSE, archived_at=NULL, include_in_net_worth=TRUE.
        savings = db.execute(
            """
            SELECT archived, archived_at, include_in_net_worth
            FROM core.dim_accounts
            WHERE account_id = ?
            """,
            [savings_id],
        ).fetchone()
        assert savings is not None, "SAVINGS1 must still exist in dim_accounts"
        savings_archived, savings_archived_at, savings_include = savings
        assert savings_archived is False, "SAVINGS1.archived must remain FALSE"
        assert savings_archived_at is None, "SAVINGS1.archived_at must remain NULL"
        assert savings_include is True, "SAVINGS1.include_in_net_worth must remain TRUE"

        # --- Requirement 9: net-worth eligibility is date-scoped, not retroactive ---
        # The fixture's daily spine has exactly one balance_date (derived from
        # the OFX statement date, not observed-and-pasted from a report read).
        spine_date = db.execute(
            "SELECT DISTINCT balance_date FROM core.fct_balances_daily"
        ).fetchall()
        assert spine_date == [(date(2026, 1, 31),)], (
            f"expected exactly one fixture balance_date, got {spine_date}"
        )
        balance_date = spine_date[0][0]

        # reports.net_worth is a VIEW that re-evaluates on every read. archived_at
        # is stamped with today's date (asserted above), which is *after* the
        # fixture's balance_date, so the eligibility filter's
        # `archived_at IS NOT NULL AND balance_date <= archived_at` arm still
        # counts CHECKING1 on this date: account_count stays 2, not 1.
        assert balance_date <= checking_archived_at, (
            "this assertion only proves what it claims when the fixture date "
            "actually precedes today's stamped archived_at"
        )
        on_or_before_nw = db.execute(
            "SELECT account_count FROM reports.net_worth WHERE balance_date = ?",
            [balance_date],
        ).fetchone()
        assert on_or_before_nw is not None and on_or_before_nw[0] == 2, (
            "Expected account_count=2 on/before archived_at (CHECKING1 still "
            f"counts retroactively), got {on_or_before_nw}"
        )

        # Exercise the exclusion side of the same rule. AccountService.archive()
        # always stamps archived_at=today, so there is no production path to a
        # historical archive date within this test; back it up past the
        # fixture's balance_date directly (the mechanism under test here is the
        # read-side date comparison, already isolated from the write path by
        # the assertions above) and restate so core.dim_accounts picks it up.
        backdated_archived_at = date(2020, 1, 1)
        assert backdated_archived_at < balance_date, (
            "backdated archived_at must actually precede the fixture's balance_date"
        )
        db.execute(
            "UPDATE app.account_settings SET archived_at = ? WHERE account_id = ?",
            [backdated_archived_at, checking_id],
        )
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=_ARCHIVE_RESTATE_MODELS,
                auto_apply=True,
                no_prompts=True,
            )

        after_nw = db.execute(
            "SELECT account_count FROM reports.net_worth WHERE balance_date = ?",
            [balance_date],
        ).fetchone()
        assert after_nw is not None and after_nw[0] == 1, (
            "Expected account_count=1 once archived_at precedes the balance_date "
            f"(CHECKING1 excluded, SAVINGS1 only), got {after_nw}"
        )

        # AccountService.list_accounts() default hides archived → 1 result.
        default_list = svc.list_accounts(include_archived=False)
        assert len(default_list.rows) == 1, (
            f"list_accounts() default: expected 1 account, got {len(default_list.rows)}"
        )
        assert default_list.rows[0].account_id == savings_id, (
            f"Expected SAVINGS1 in default list, got {default_list.rows[0].account_id}"
        )

        # list_accounts(include_archived=True) returns both accounts.
        full_list = svc.list_accounts(include_archived=True)
        assert len(full_list.rows) == 2, (
            f"list_accounts(include_archived=True): expected 2, got {len(full_list.rows)}"
        )
        ids = {row.account_id for row in full_list.rows}
        assert ids == {checking_id, savings_id}, (
            f"Expected both accounts in --include-archived list, got {ids}"
        )
