/*
Purpose: Extract and aggregate daily token balances for a specific token
Model Type: Base State Extraction
Dependencies: solana_utils.daily_balances (Dune blockchain data table)
Output: One row per (token, wallet, day) with end-of-day balance
Query Link: https://dune.com/queries/XXXXXX

This is the foundation query that filters the massive Solana blockchain dataset
(~5 billion rows) down to a single token's daily wallet balances. All downstream
models depend on this filtered view.

Aggregation Logic:
Some wallet owners control multiple Associated Token Accounts (ATAs) on Solana.
We sum balances across all ATAs per owner to get true wallet-level holdings.

Performance Note:
This query scans billions of rows. Materializing this layer is critical for
downstream query performance. Without materialization, every analysis would
repeat this expensive table scan.

Parameters (Manual Update Required):
- token_address: Solana token mint address
- start_date: Analysis start date
- end_date: Analysis end date (typically CURRENT_DATE)
*/

WITH params AS (
    SELECT
        '{{token_address}}' AS token_address,
        DATE '{{start_date}}' AS start_date,
        DATE '{{end_date}}' AS end_date
),

-- Extract daily balances for specified token and date range
-- Aggregates across Associated Token Accounts (ATAs) per owner
raw_state AS (
    SELECT
        day,
        token_mint_address AS token_address,
        token_balance_owner AS owner,
        SUM(token_balance) AS eod_balance  -- Sum across multiple ATAs
    FROM solana_utils.daily_balances
    WHERE token_mint_address = (SELECT token_address FROM params)
      AND day BETWEEN (SELECT start_date FROM params)
                  AND (SELECT end_date FROM params)
    GROUP BY day, token_mint_address, token_balance_owner
)

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

SELECT
    day,
    token_address,
    owner,
    CAST(eod_balance AS DOUBLE) AS eod_balance  -- Ensure floating-point precision
FROM raw_state
ORDER BY day, owner;
