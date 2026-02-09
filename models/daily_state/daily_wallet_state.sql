-- Canonical daily wallet state
-- One row per (token, wallet, day) with known end-of-day balance
-- Reference table for all future models 
WITH params AS (
    SELECT
        '{{token_address}}' AS token_address,
        DATE '{{start_date}}' AS start_date,
        DATE '{{end_date}}' AS end_date
),

raw_state AS (
    SELECT
        day,
        token_mint_address AS token_address,
        token_balance_owner AS owner,
        SUM(token_balance) AS eod_balance
    FROM solana_utils.daily_balances
    WHERE token_mint_address = (SELECT token_address FROM params)
      AND day BETWEEN (SELECT start_date FROM params)
                  AND (SELECT end_date FROM params)
    GROUP BY day, token_mint_address, token_balance_owner
)

SELECT
    day,
    token_address,
    owner,
    CAST(eod_balance AS DOUBLE) AS eod_balance
FROM raw_state;
