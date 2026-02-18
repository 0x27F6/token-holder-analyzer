/*
Purpose: A base for future analytical architecture that leverages the canonical end of day(EOD) snapshotted balance changes
recorded by the daily_balances table. By cross referencing canonical EOD snapshots with verified trader activity and liquidity pool
infrascturcture we can better classify wallet address type. Allowing for a more granular read into user behavior that traditional naive approaches 
forgo. 

DUNE: https://dune.com/queries/6683290

Tables: 
  1. solana_utils.daily_balances
  2. dex_solana.trades

Dependencies:
  1. Liqudity Pool Finder: https://dune.com/queries/6703395
  2. Dex Trader Wallets: https://dune.com/queries/6694162


*/



-- Canonical daily wallet state
-- One row per (token, wallet, day) with known end-of-day balance
-- Reference table for all future models 
WITH params AS (
    SELECT
        'METAwkXcqyXKy1AtsSgJ8JiUHwGCafnZL38n3vYmeta' AS token_address,
        DATE '2025-08-15' AS start_date,
        CURRENT_DATE      AS end_date,
        5                 AS threshold,
        22684699          AS total_supply
),
daily_state AS (
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
),
-- ============================================================================
-- STATE ENRICHMENT & WALLET CLASSIFICATION
-- ============================================================================
prev_state AS (
    SELECT 
        day, 
        owner,
        eod_balance,
        LAG(eod_balance) OVER(PARTITION BY owner ORDER BY day) AS prev_balance
    FROM daily_state
),
-- remove intraday noise
state_filter AS (
    SELECT
        day,
        owner,
        eod_balance,
        prev_balance
    FROM prev_state
    WHERE NOT (eod_balance = 0 AND (prev_balance = 0 OR prev_balance IS NULL))
),
-- ============================================================================
-- WALLET LABELING
-- LP infrastructure, DEX traders, and unlabeled (transfer-only) wallets
-- Joined once upstream so all downstream models inherit classification
-- ============================================================================
lp_addresses AS (
    SELECT trade_source AS address 
    FROM dune.research_onchain.result_lp_finder
    
    UNION
    
    SELECT project_program_id 
    FROM dune.research_onchain.result_lp_finder
    
    UNION
    
    SELECT project_main_id 
    FROM dune.research_onchain.result_lp_finder
    
    UNION
    -- unique AMM for metaDAO that is not labeled by dune 
    SELECT 'CUPoiqkK4hxyCiJcLC4yE9AtJP1MoV1vFV2vx3jqwWeS' 
),

dex_traders AS (
    SELECT DISTINCT trader_id
    FROM dune.research_onchain.result_meta_dao_dex_base
),
-- ============================================================================
-- THRESHOLD & WALLET TYPE CLASSIFICATION
-- ============================================================================
classified_state AS (
    SELECT 
        sf.day,
        sf.owner,
        sf.eod_balance,
        sf.prev_balance,
        CASE 
            WHEN sf.eod_balance < (SELECT threshold FROM params) THEN 'below_threshold'
            ELSE 'above_threshold'
        END AS threshold_label,
        CASE
            WHEN lp.address IS NOT NULL THEN 'lp'
            WHEN dex.trader_id IS NOT NULL THEN 'dex_trader'
            ELSE 'transfer_only'
        END AS wallet_type
    FROM state_filter sf
    LEFT JOIN lp_addresses lp
        ON sf.owner = lp.address
    LEFT JOIN dex_traders dex
        ON sf.owner = dex.trader_id
),
-- ============================================================================
-- PRICE
-- ============================================================================
price_base AS (
    SELECT 
        block_date,
        block_time,
        token_bought_symbol,
        token_bought_amount,
        token_sold_amount,
        amount_usd,
        CASE 
            WHEN token_bought_mint_address = (SELECT token_address FROM params) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_buy
    FROM dex_solana.trades
    WHERE block_date BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
      AND (token_bought_mint_address = (SELECT token_address FROM params)
      OR token_sold_mint_address = (SELECT token_address FROM params))
),
end_day_price AS (
    SELECT
        block_date AS day,
        MAX_BY(
            amount_usd / NULLIF(
                CASE 
                    WHEN is_buy THEN token_bought_amount
                    ELSE token_sold_amount
                END, 0),
            block_time
        ) AS eod_price
    FROM price_base
    WHERE amount_usd > 0
    GROUP BY 1
),
-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================
final_output AS (
    SELECT 
        cs.day,
        cs.owner,
        cs.eod_balance,
        cs.prev_balance,
        cs.threshold_label,
        cs.wallet_type,
        ed.eod_price,
        p.start_date,
        p.end_date,
        p.threshold,
        p.total_supply,
        p.token_address
    FROM classified_state cs
    LEFT JOIN end_day_price ed
        ON ed.day = cs.day
    CROSS JOIN params p
)

SELECT * FROM final_output
