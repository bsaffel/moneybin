"""Direct raw-ledger seeding helpers for the investment scenario suites.

Underscore-prefixed so pytest never collects it as a test module (mirrors
``_harnesses.py`` / ``_runner``). The scenario tests seed
``raw.manual_investment_transactions`` directly — the same seed point
``load_fixtures`` uses for cash transactions and the same shape
``InvestmentService._write_rows`` inserts — then run the REAL transform and
assert the derived ``core.*`` investment models. Raw is a legitimate scenario
input (like ``app.balance_assertions`` in ``test_networth_correctness``); the
mechanism under test is the cost-basis engine + SQLMesh models, which run for
real.

The reinvest CONVENIENCE (writing the acquisition leg + paired income row
sharing an ``event_group_id``) is a service-layer write helper, unit-covered by
``test_investment_service.py::TestReinvestPairing``. The 1099-B scenario seeds
that pair directly (mirroring the documented two-row shape) and asserts the
pipeline consumes it correctly — routing through the real
``InvestmentService.record_event`` is impossible here because it resolves the
account against ``core.dim_accounts``, which an investment-only account never
populates (that table is built from cash-transaction sources).

Amounts/quantities are passed as strings and cast in-SQL to their DECIMAL target
types (never through a float — 1099-B exactness), mirroring the Task-13
integration test ``_insert_investment_txn``.
"""

from __future__ import annotations

from moneybin.database import Database

_IMPORT_ID = "scenario_import"


def insert_security(
    db: Database,
    *,
    security_id: str,
    name: str,
    security_type: str,
    cost_basis_method: str | None = None,
    currency_code: str = "USD",
) -> None:
    """Seed one ``app.securities`` catalog row (drives per-security method election).

    ``cost_basis_method`` NULL falls back to the account default, then global
    FIFO — the resolution chain the loader's ``method_for`` implements.
    """
    db.execute(
        """
        INSERT INTO app.securities
            (security_id, name, security_type, cost_basis_method, currency_code)
        VALUES (?, ?, ?, ?, ?)
        """,  # noqa: S608  # test fixture insert, parameterized values
        [security_id, name, security_type, cost_basis_method, currency_code],
    )


def insert_security_link(
    db: Database,
    *,
    link_id: str,
    security_id: str,
    ref_value: str,
    source_type: str = "plaid",
    ref_kind: str = "plaid_security_id",
    decided_at: str = "2024-01-01 00:00:00",
) -> None:
    """Seed one accepted ``app.security_links`` binding (provider ref → canonical).

    A price observation resolves through this binding in
    ``prep.stg_security_prices``, so the binding is a PRECONDITION of the
    valuation chain, not the mechanism under test — the same standing
    ``insert_security`` has for the catalog. The review flow that *produces* a
    binding (``SecurityLinksService`` propose → accept) is covered by its own
    unit tests; seeding an already-accepted row here is the equivalent of a
    user who bound the security some time ago.
    """
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type, status,
             decided_by, decided_at)
        VALUES (?, ?, ?, ?, ?, 'accepted', 'user', ?::TIMESTAMP)
        """,  # noqa: S608  # test fixture insert, parameterized values
        [link_id, security_id, ref_kind, ref_value, source_type, decided_at],
    )


def insert_security_price(
    db: Database,
    *,
    provider_security_key: str,
    price_date: str,
    close: str,
    source: str = "plaid",
    source_origin: str = "item_scenario",
    quote_currency: str = "USD",
    price_basis: str = "raw",
    extracted_at: str = "2024-08-01 00:00:00",
) -> None:
    """Seed one ``raw.security_prices`` close observation (the price-feed input).

    Keyed by the PROVIDER's own security id — resolution to the canonical
    ``security_id`` happens in staging, through ``app.security_links``. ``close``
    is passed as a string and cast in-SQL so the exact value reaches DuckDB
    without a float round-trip, matching ``insert_event``.
    """
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source_type,
             source_origin, close, price_basis, extracted_at)
        VALUES (?, ?::DATE, ?, ?, ?, ?::DECIMAL(28,10), ?, ?::TIMESTAMP)
        """,  # noqa: S608  # test fixture insert, parameterized values
        [
            provider_security_key,
            price_date,
            quote_currency,
            source,
            source_origin,
            close,
            price_basis,
            extracted_at,
        ],
    )


def insert_event(
    db: Database,
    *,
    investment_transaction_id: str,
    account_id: str,
    type_: str,
    trade_date: str,
    security_id: str | None = None,
    security_ref: str | None = None,
    subtype: str | None = None,
    event_group_id: str | None = None,
    quantity: str | None = None,
    price: str | None = None,
    amount: str | None = None,
    fees: str | None = None,
    original_acquisition_date: str | None = None,
    currency_code: str = "USD",
    created_at: str = "2024-01-01 00:00:00",
) -> None:
    """Seed one ``raw.manual_investment_transactions`` row (the ledger input).

    ``investment_transaction_id`` doubles as the ``source_transaction_id`` PK —
    both must be unique per event. The engine hashes the
    ``investment_transaction_id`` (carried through staging → core) into
    ``lot_id`` / ``disposal_txn_id`` / ``realized_gain_id``. Decimal columns are
    cast in-SQL so the exact string value reaches DuckDB without a float
    round-trip.
    """
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions
            (source_transaction_id, import_id, account_id, security_id,
             security_ref, type, subtype, event_group_id, trade_date,
             original_acquisition_date, quantity, price, amount, fees,
             currency_code, created_at, created_by, investment_transaction_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::DATE, ?::DATE,
                ?::DECIMAL(28,10), ?::DECIMAL(28,10), ?::DECIMAL(18,2),
                ?::DECIMAL(18,2), ?, ?::TIMESTAMP, 'cli', ?)
        """,  # noqa: S608  # test fixture insert, parameterized values
        [
            investment_transaction_id,
            _IMPORT_ID,
            account_id,
            security_id,
            security_ref,
            type_,
            subtype,
            event_group_id,
            trade_date,
            original_acquisition_date,
            quantity,
            price,
            amount,
            fees,
            currency_code,
            created_at,
            investment_transaction_id,
        ],
    )
