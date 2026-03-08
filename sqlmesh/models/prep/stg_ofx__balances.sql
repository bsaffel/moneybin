MODEL (
    name prep.stg_ofx__balances,
    kind VIEW
);

SELECT
    account_id,
    CAST(statement_start_date AS DATE) AS statement_start_date,
    CAST(statement_end_date AS DATE) AS statement_end_date,
    ledger_balance,
    CAST(ledger_balance_date AS DATE) AS ledger_balance_date,
    available_balance,
    source_file,
    extracted_at,
    loaded_at
FROM raw.ofx_balances
