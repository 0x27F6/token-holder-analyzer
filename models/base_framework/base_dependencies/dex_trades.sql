/*
Purpose: Aggregates all DEX trading activity for the target token into daily per-wallet buy/sell summaries.
Captures directional trade flow (tokens bought vs sold) and USD volume at the wallet level, enabling 
downstream analysis of trade-level market activity separate from EOD balance snapshots.

Also serves as the source of distinct trader_id addresses used for wallet classification in the base model.

DUNE: https://dune.com/queries/6694162

Tables:
  1. dex_solana.trades

Dependencies:
  None (standalone)

Output:
  One row per (day, trader_id) with:
    - tokens_bought / tokens_sold: directional token flow
    - buys_usd / sells_usd: USD volume by side
    - usd_delta: net USD pressure (positive = buy dominant)
    - token_delta: net token flow (positive = net accumulation)
*/

WITH 
params AS(
    SELECT 
        'META' AS ticker
),
-- Isolate all DEX transactions involving the target token (either side of the swap)
base AS(
    SELECT 
        block_date,
        trader_id,
        token_bought_amount,
        token_sold_amount,
        amount_usd,
        token_bought_symbol,
        token_sold_symbol
    FROM dex_solana.trades
    WHERE (token_bought_mint_address = 'METAwkXcqyXKy1AtsSgJ8JiUHwGCafnZL38n3vYmeta'
      OR token_sold_mint_address = 'METAwkXcqyXKy1AtsSgJ8JiUHwGCafnZL38n3vYmeta')
      AND block_date BETWEEN DATE '2025-08-15' AND CURRENT_DATE
      AND amount_usd > 0
),
-- Split into buy/sell rows based on which side of the swap the target token was on
-- Buy = token was bought, Sell = token was sold
in_out AS(
    SELECT
        block_date AS day,
        trader_id,
        token_bought_amount AS token_amount,
        amount_usd,
        'buy' AS txn_type
    FROM base
    WHERE token_bought_symbol = (SELECT ticker FROM params)
    
    UNION ALL
    
    SELECT
        block_date AS day,
        trader_id,
        token_sold_amount AS token_amount,
        amount_usd,
        'sell' AS txn_type
    FROM base
    WHERE token_sold_symbol = (SELECT ticker FROM params) 
),
-- Aggregate to daily per-wallet level with directional metrics
buy_sell_daily AS (
    SELECT
        day,
        trader_id,
        SUM(CASE WHEN txn_type = 'buy' THEN token_amount ELSE 0 END) AS tokens_bought,
        SUM(CASE WHEN txn_type = 'sell' THEN token_amount ELSE 0 END) AS tokens_sold,
        SUM(CASE WHEN txn_type = 'buy' THEN amount_usd ELSE 0 END) AS buys_usd,
        SUM(CASE WHEN txn_type = 'sell' THEN amount_usd ELSE 0 END) AS sells_usd,
        -- Positive = net buy pressure, Negative = net sell pressure
        SUM(CASE WHEN txn_type = 'buy' THEN amount_usd ELSE -amount_usd END) AS usd_delta,
        SUM(CASE WHEN txn_type = 'buy' THEN token_amount ELSE -token_amount END) AS token_delta
    FROM in_out
    GROUP BY day, trader_id
)

SELECT 
    day, 
    trader_id,
    tokens_bought,
    tokens_sold,
    buys_usd,
    sells_usd,
    usd_delta,
    token_delta
FROM buy_sell_daily
ORDER BY day
  
